-- =============================================================================
-- FHIR-to-OMOP CDM — Transformation Stored Procedures (custom-built)
--
-- FHIR parsing: uses Snowflake VARIANT + LATERAL FLATTEN for JSON bundles,
--   Python xml.etree.ElementTree for XML bundles,
--   Pure Python HL7v2 segment parser with FHIR R4 conversion (auto-detected)
--   (inspired by Tuva Health's FHIR Inferno patterns, Apache 2.0)
-- OMOP mapping: original logic mapping FHIR R4 resources to OMOP CDM v5.4
--   tables via vocabulary crosswalk lookups
-- Multi-cloud: pure Snowflake SQL + Python, no cloud-specific features
--
-- NOTE: All mappers use the CTE pattern for LATERAL FLATTEN + LEFT JOIN
--   because Snowflake does not support LEFT JOIN LATERAL FLATTEN with ON clause.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- FHIR Bundle Parser — extracts resources from FHIR R4 JSON, XML, or HL7v2
-- Auto-detects format: JSON (starts with '{'), XML ('<'), or HL7v2 ('MSH|')
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.parse_fhir_bundles(
    source_table VARCHAR,
    json_column  VARCHAR DEFAULT 'BUNDLE_DATA',
    bundle_id_column VARCHAR DEFAULT 'BUNDLE_ID'
)
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
import json
import xml.etree.ElementTree as ET

NS = 'http://hl7.org/fhir'
NS_BRACKET = f'{{{NS}}}'

NUMERIC_VALUE_CONTEXTS = {
    'valueQuantity', 'quantity', 'valueRange', 'valueRatio',
    'position', 'doseQuantity', 'rateQuantity', 'simpleQuantity',
    'low', 'high', 'numerator', 'denominator', 'factor',
    'net', 'unitPrice', 'amount', 'total', 'payment',
    'dose', 'count', 'frequency', 'period', 'duration',
    'numberOfSeries', 'numberOfInstances', 'sequence'
}

ARRAY_FIELDS = {
    'entry', 'extension', 'coding', 'identifier', 'name', 'telecom',
    'address', 'contact', 'communication', 'link', 'contained',
    'category', 'performer', 'participant', 'insurance', 'item',
    'diagnosis', 'procedure', 'supportingInfo', 'qualification',
    'series', 'instance', 'referenceRange', 'component', 'activity',
    'note', 'reaction', 'stage', 'evidence', 'bodySite', 'reasonCode',
    'reasonReference', 'modifierExtension', 'type', 'udiCarrier',
    'prefix', 'given', 'suffix', 'line', 'dosageInstruction',
    'doseAndRate', 'total', 'location', 'result', 'basedOn',
    'partOf', 'focus', 'role', 'manifestation', 'deviceName',
    'repeat', 'hospitalization'
}

def strip_ns(tag):
    return tag.replace(NS_BRACKET, '') if NS_BRACKET in tag else tag

def fhir_xml_element_to_json(elem, parent_tag=''):
    tag = strip_ns(elem.tag)
    result = {}

    if elem.attrib.get('value') is not None:
        val = elem.attrib['value']
        if val.lower() in ('true', 'false'):
            return val.lower() == 'true'
        if tag == 'value' and parent_tag.lower() in {p.lower() for p in NUMERIC_VALUE_CONTEXTS}:
            try:
                return float(val) if '.' in val else int(val)
            except ValueError:
                return val
        if tag in ('latitude', 'longitude', 'numberOfSeries', 'numberOfInstances', 'sequence'):
            try:
                return float(val) if '.' in val else int(val)
            except ValueError:
                return val
        return val

    if elem.attrib.get('url') is not None:
        result['url'] = elem.attrib['url']

    children_by_tag = {}
    for child in elem:
        child_tag = strip_ns(child.tag)
        child_val = fhir_xml_element_to_json(child, parent_tag=tag)
        if child_tag not in children_by_tag:
            children_by_tag[child_tag] = []
        children_by_tag[child_tag].append(child_val)

    for ctag, cvals in children_by_tag.items():
        if ctag in ARRAY_FIELDS or len(cvals) > 1:
            result[ctag] = cvals
        else:
            result[ctag] = cvals[0]

    return result

def parse_fhir_xml_bundle(xml_string):
    root = ET.fromstring(xml_string)
    resources = []

    entries = root.findall(f'{NS_BRACKET}entry')
    if not entries:
        entries = root.findall('entry')

    for entry in entries:
        full_url_elem = entry.find(f'{NS_BRACKET}fullUrl')
        if full_url_elem is None:
            full_url_elem = entry.find('fullUrl')
        full_url = full_url_elem.attrib.get('value', '') if full_url_elem is not None else ''

        resource_elem = entry.find(f'{NS_BRACKET}resource')
        if resource_elem is None:
            resource_elem = entry.find('resource')
        if resource_elem is None:
            continue

        for child in resource_elem:
            resource_type = strip_ns(child.tag)
            resource_json = fhir_xml_element_to_json(child, parent_tag='resource')
            resource_json['resourceType'] = resource_type

            rid_elem = child.find(f'{NS_BRACKET}id')
            if rid_elem is None:
                rid_elem = child.find('id')
            resource_id = None
            if rid_elem is not None and rid_elem.attrib.get('value'):
                resource_id = rid_elem.attrib['value']
            elif full_url:
                if full_url.startswith('urn:uuid:'):
                    resource_id = full_url[9:]
                elif '/' in full_url:
                    resource_id = full_url.rsplit('/', 1)[-1]

            resources.append({
                'resource_id': resource_id,
                'resource_type': resource_type,
                'resource_json': resource_json
            })

    return resources

def run(session, source_table: str, json_column: str, bundle_id_column: str) -> str:
    session.sql("CREATE SCHEMA IF NOT EXISTS app_state").collect()
    session.sql("""
        CREATE TABLE IF NOT EXISTS app_state.fhir_resources (
            resource_id     VARCHAR(256),
            resource_type   VARCHAR(100),
            bundle_id       VARCHAR(256),
            resource_json   VARIANT,
            parsed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

    session.sql("TRUNCATE TABLE IF EXISTS app_state.fhir_resources").collect()

    sample = session.sql(f"""
        SELECT
               SUBSTR(TRIM({json_column}::VARCHAR), 1, 4) AS first_chars
        FROM {source_table}
        LIMIT 1
    """).collect()

    if not sample:
        return "No data found in source table"

    first_chars = sample[0]['FIRST_CHARS']
    is_xml = (first_chars[:1] == '<')
    is_hl7v2 = (first_chars[:4] == 'MSH|' or first_chars[:3] == 'MSH')

    if is_hl7v2:
        result = session.sql(f"""
            CALL core.parse_hl7v2_to_fhir('{source_table}', '{json_column}', '{bundle_id_column}')
        """).collect()
        return result[0][0] if result else "HL7v2 parsing completed"

    if is_xml:
        rows = session.sql(f"""
            SELECT {bundle_id_column}::VARCHAR AS bid, {json_column}::VARCHAR AS xml_data
            FROM {source_table}
        """).collect()

        all_values = []
        errors = 0
        for row in rows:
            bid = row['BID']
            xml_data = row['XML_DATA']
            if not xml_data or not xml_data.strip():
                continue
            try:
                resources = parse_fhir_xml_bundle(xml_data.strip())
                for res in resources:
                    all_values.append((
                        (res['resource_id'] or ''),
                        res['resource_type'],
                        str(bid),
                        res['resource_json']
                    ))
            except Exception as e:
                all_values.append((
                    'ERROR',
                    'ParseError',
                    str(bid),
                    {"error": str(e)}
                ))
                errors += 1

        if all_values:
            batch_size = 500
            for i in range(0, len(all_values), batch_size):
                batch = all_values[i:i+batch_size]
                union_parts = []
                for rid, rtype, bid, rjson in batch:
                    rj = json.dumps(rjson).replace("'", "''")
                    rid_safe = rid.replace("'", "''")
                    bid_safe = bid.replace("'", "''")
                    union_parts.append(
                        f"SELECT '{rid_safe}', '{rtype}', '{bid_safe}', PARSE_JSON('{rj}')"
                    )
                union_sql = " UNION ALL ".join(union_parts)
                session.sql(f"""
                    INSERT INTO app_state.fhir_resources
                        (resource_id, resource_type, bundle_id, resource_json)
                    {union_sql}
                """).collect()

        count = session.sql("SELECT COUNT(*) AS cnt FROM app_state.fhir_resources WHERE resource_type != 'ParseError'").collect()[0]['CNT']
        msg = f"Parsed {count} FHIR resources from XML bundles in {source_table}"
        if errors > 0:
            msg += f" ({errors} bundle parse errors)"
        return msg
    else:
        extract_sql = f"""
            INSERT INTO app_state.fhir_resources (resource_id, resource_type, bundle_id, resource_json)
            SELECT
                r.value:resource:id::VARCHAR           AS resource_id,
                r.value:resource:resourceType::VARCHAR  AS resource_type,
                src.{bundle_id_column}                  AS bundle_id,
                r.value:resource                        AS resource_json
            FROM {source_table} src,
                LATERAL FLATTEN(input => src.{json_column}:entry) r
            WHERE r.value:resource:resourceType IS NOT NULL
        """
        session.sql(extract_sql).collect()
        count = session.sql("SELECT COUNT(*) AS cnt FROM app_state.fhir_resources").collect()[0]['CNT']
        return f"Parsed {count} FHIR resources from {source_table}"
$$;
GRANT USAGE ON PROCEDURE core.parse_fhir_bundles(VARCHAR, VARCHAR, VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- HL7v2 → FHIR R4 Converter — parses raw HL7v2 pipe-delimited messages and
-- converts segments to FHIR R4 JSON resources, then inserts into
-- app_state.fhir_resources so all 23 downstream OMOP mappers work unchanged.
--
-- Supported HL7v2 segments → FHIR resources:
--   PID → Patient          PV1 → Encounter
--   DG1 → Condition        OBX → Observation
--   OBR → DiagnosticReport RXA → MedicationAdministration
--   PR1 → Procedure        AL1 → AllergyIntolerance
--   IN1 → Coverage/Claim   NK1 → RelatedPerson
--
-- Pure Python, zero external dependencies — works in Native App context.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.parse_hl7v2_to_fhir(
    source_table VARCHAR,
    message_column VARCHAR DEFAULT 'RAW_MESSAGE',
    message_id_column VARCHAR DEFAULT 'MESSAGE_ID'
)
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
import json
import hashlib

def make_id(prefix, *parts):
    raw = '|'.join(str(p) for p in parts if p)
    return prefix + '-' + hashlib.md5(raw.encode()).hexdigest()[:12]

def parse_hl7v2_message(raw):
    lines = raw.strip().replace('\r\n', '\r').replace('\n', '\r').split('\r')
    lines = [l for l in lines if l.strip()]
    if not lines:
        return None, None, []

    msg_type = ''
    trigger = ''
    field_sep = '|'
    comp_sep = '^'
    rep_sep = '~'
    esc_char = '\\'
    sub_sep = '&'

    segments = []
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith('MSH'):
            if len(line) > 3:
                field_sep = line[3]
            encoding_chars = line[4:8] if len(line) > 7 else '^~\\&'
            if len(encoding_chars) >= 1: comp_sep = encoding_chars[0]
            if len(encoding_chars) >= 2: rep_sep = encoding_chars[1]
            if len(encoding_chars) >= 3: esc_char = encoding_chars[2]
            if len(encoding_chars) >= 4: sub_sep = encoding_chars[3]

            fields = line.split(field_sep)
            msh_fields = [field_sep] + fields[1:]

            if len(msh_fields) > 8:
                mt = msh_fields[8]
                if comp_sep in mt:
                    parts = mt.split(comp_sep)
                    msg_type = parts[0]
                    trigger = parts[1] if len(parts) > 1 else ''
                else:
                    msg_type = mt

            segments.append(('MSH', msh_fields))
        else:
            fields = line.split(field_sep)
            seg_type = fields[0] if fields else ''
            segments.append((seg_type, fields))

    return msg_type, trigger, segments

def get_field(fields, idx, default=''):
    if idx < len(fields) and fields[idx]:
        return fields[idx]
    return default

def get_component(field_val, comp_idx, comp_sep='^', default=''):
    if not field_val:
        return default
    parts = field_val.split(comp_sep)
    if comp_idx < len(parts) and parts[comp_idx]:
        return parts[comp_idx]
    return default

def pid_to_patient(fields, msg_id):
    resource_id = make_id('pat', msg_id, get_field(fields, 3))

    pid3 = get_field(fields, 3)
    mrn = get_component(pid3, 0)

    pid5 = get_field(fields, 5)
    family = get_component(pid5, 0)
    given = get_component(pid5, 1)
    middle = get_component(pid5, 2)

    dob_raw = get_field(fields, 7)
    birth_date = ''
    if len(dob_raw) >= 8:
        birth_date = f"{dob_raw[:4]}-{dob_raw[4:6]}-{dob_raw[6:8]}"

    gender_code = get_field(fields, 8)
    gender_map = {'M': 'male', 'F': 'female', 'O': 'other', 'U': 'unknown', 'A': 'other', 'N': 'other'}
    gender = gender_map.get(gender_code.upper(), 'unknown')

    pid10 = get_field(fields, 10)
    race_code = get_component(pid10, 0)

    pid11 = get_field(fields, 11)
    addr_line = get_component(pid11, 0)
    addr_city = get_component(pid11, 2)
    addr_state = get_component(pid11, 3)
    addr_zip = get_component(pid11, 4)

    pid22 = get_field(fields, 22)
    ethnicity_code = get_component(pid22, 0)

    resource = {
        'resourceType': 'Patient',
        'id': resource_id,
        'identifier': [{'system': 'http://hospital.example.org/mrn', 'value': mrn}] if mrn else [],
        'name': [{}],
        'gender': gender,
        'extension': []
    }

    name = {}
    if family: name['family'] = family
    if given:
        givens = [given]
        if middle: givens.append(middle)
        name['given'] = givens
    resource['name'] = [name] if name else []

    if birth_date:
        resource['birthDate'] = birth_date

    if addr_line or addr_city or addr_state or addr_zip:
        addr = {}
        if addr_line: addr['line'] = [addr_line]
        if addr_city: addr['city'] = addr_city
        if addr_state: addr['state'] = addr_state
        if addr_zip: addr['postalCode'] = addr_zip
        resource['address'] = [addr]

    race_display_map = {
        '2106-3': 'White', '2054-5': 'Black or African American',
        '2076-8': 'Native Hawaiian or Other Pacific Islander',
        '2028-9': 'Asian', '1002-5': 'American Indian or Alaska Native',
        '2131-1': 'Other Race', '2135-2': 'Hispanic or Latino'
    }
    if race_code:
        resource['extension'].append({
            'url': 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race',
            'extension': [
                {'url': 'ombCategory', 'valueCoding': {
                    'system': 'urn:oid:2.16.840.1.113883.6.238',
                    'code': race_code,
                    'display': race_display_map.get(race_code, race_code)
                }},
                {'url': 'text', 'valueString': race_display_map.get(race_code, race_code)}
            ]
        })

    if ethnicity_code:
        eth_display = 'Hispanic or Latino' if ethnicity_code in ('2135-2', 'H') else 'Not Hispanic or Latino'
        eth_code = '2135-2' if ethnicity_code in ('2135-2', 'H') else '2186-5'
        resource['extension'].append({
            'url': 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity',
            'extension': [
                {'url': 'ombCategory', 'valueCoding': {
                    'system': 'urn:oid:2.16.840.1.113883.6.238',
                    'code': eth_code,
                    'display': eth_display
                }},
                {'url': 'text', 'valueString': eth_display}
            ]
        })

    if not resource['extension']:
        del resource['extension']

    return resource_id, 'Patient', resource

def pv1_to_encounter(fields, msg_id, patient_ref):
    resource_id = make_id('enc', msg_id, get_field(fields, 19, get_field(fields, 1)))

    patient_class = get_field(fields, 2, 'I')
    class_map = {
        'I': ('IMP', 'inpatient encounter'),
        'O': ('AMB', 'ambulatory'),
        'E': ('EMER', 'emergency'),
        'P': ('PRENC', 'pre-admission'),
        'R': ('IMP', 'inpatient encounter'),
        'B': ('AMB', 'ambulatory'),
    }
    cls_code, cls_display = class_map.get(patient_class.upper(), ('IMP', 'inpatient encounter'))

    pv1_44 = get_field(fields, 44)
    admit_date = ''
    if len(pv1_44) >= 8:
        admit_date = f"{pv1_44[:4]}-{pv1_44[4:6]}-{pv1_44[6:8]}"

    pv1_45 = get_field(fields, 45)
    discharge_date = ''
    if len(pv1_45) >= 8:
        discharge_date = f"{pv1_45[:4]}-{pv1_45[4:6]}-{pv1_45[6:8]}"

    resource = {
        'resourceType': 'Encounter',
        'id': resource_id,
        'status': 'finished',
        'class': {
            'system': 'http://terminology.hl7.org/CodeSystem/v3-ActCode',
            'code': cls_code,
            'display': cls_display
        },
        'subject': {'reference': f'Patient/{patient_ref}'}
    }

    if admit_date or discharge_date:
        period = {}
        if admit_date: period['start'] = admit_date
        if discharge_date: period['end'] = discharge_date
        resource['period'] = period

    pv1_3 = get_field(fields, 3)
    if pv1_3:
        loc_parts = pv1_3.split('^') if '^' in pv1_3 else [pv1_3]
        loc_display = ' '.join(p for p in loc_parts if p)
        if loc_display:
            resource['location'] = [{'location': {'display': loc_display}}]

    return resource_id, 'Encounter', resource

def dg1_to_condition(fields, msg_id, patient_ref, encounter_ref=None):
    dg1_3 = get_field(fields, 3)
    code = get_component(dg1_3, 0)
    display = get_component(dg1_3, 1)
    code_system_id = get_component(dg1_3, 2)

    code_system_map = {
        'I10': 'http://hl7.org/fhir/sid/icd-10-cm',
        'I9': 'http://hl7.org/fhir/sid/icd-9-cm',
        'I9C': 'http://hl7.org/fhir/sid/icd-9-cm',
        'SCT': 'http://snomed.info/sct',
        'SNM': 'http://snomed.info/sct',
    }
    code_system = code_system_map.get(code_system_id, f'urn:oid:{code_system_id}' if code_system_id else 'http://hl7.org/fhir/sid/icd-10-cm')

    resource_id = make_id('cond', msg_id, code, patient_ref)

    dg1_5 = get_field(fields, 5)
    onset_date = ''
    if len(dg1_5) >= 8:
        onset_date = f"{dg1_5[:4]}-{dg1_5[4:6]}-{dg1_5[6:8]}"

    resource = {
        'resourceType': 'Condition',
        'id': resource_id,
        'subject': {'reference': f'Patient/{patient_ref}'},
        'code': {
            'coding': [{'system': code_system, 'code': code, 'display': display or code}],
            'text': display or code
        },
        'clinicalStatus': {
            'coding': [{'system': 'http://terminology.hl7.org/CodeSystem/condition-clinical', 'code': 'active'}]
        }
    }

    if onset_date:
        resource['onsetDateTime'] = onset_date
    if encounter_ref:
        resource['encounter'] = {'reference': f'Encounter/{encounter_ref}'}

    return resource_id, 'Condition', resource

def obx_to_observation(fields, msg_id, patient_ref, encounter_ref=None):
    obx_2 = get_field(fields, 2)
    obx_3 = get_field(fields, 3)
    code = get_component(obx_3, 0)
    display = get_component(obx_3, 1)
    code_system_id = get_component(obx_3, 2)

    code_system_map = {
        'LN': 'http://loinc.org',
        'SCT': 'http://snomed.info/sct',
        'SNM': 'http://snomed.info/sct',
    }
    code_system = code_system_map.get(code_system_id, f'urn:oid:{code_system_id}' if code_system_id else 'http://loinc.org')

    obx_5 = get_field(fields, 5)
    obx_6 = get_field(fields, 6)

    seq = get_field(fields, 1, '0')
    resource_id = make_id('obs', msg_id, code, seq, patient_ref)

    obx_14 = get_field(fields, 14)
    effective_date = ''
    if len(obx_14) >= 8:
        effective_date = f"{obx_14[:4]}-{obx_14[4:6]}-{obx_14[6:8]}"

    resource = {
        'resourceType': 'Observation',
        'id': resource_id,
        'status': 'final',
        'code': {
            'coding': [{'system': code_system, 'code': code, 'display': display or code}],
            'text': display or code
        },
        'subject': {'reference': f'Patient/{patient_ref}'}
    }

    if obx_2 == 'NM' and obx_5:
        try:
            val = float(obx_5)
            resource['valueQuantity'] = {'value': val}
            if obx_6:
                resource['valueQuantity']['unit'] = obx_6
                resource['valueQuantity']['system'] = 'http://unitsofmeasure.org'
        except ValueError:
            resource['valueString'] = obx_5
    elif obx_2 == 'ST' or obx_2 == 'TX':
        resource['valueString'] = obx_5
    elif obx_2 == 'CE' or obx_2 == 'CWE':
        val_code = get_component(obx_5, 0)
        val_display = get_component(obx_5, 1)
        val_sys = get_component(obx_5, 2)
        resource['valueCodeableConcept'] = {
            'coding': [{'system': val_sys or code_system, 'code': val_code, 'display': val_display or val_code}]
        }
    elif obx_5:
        resource['valueString'] = obx_5

    obx_8 = get_field(fields, 8)
    if obx_8:
        interp_map = {
            'H': 'high', 'L': 'low', 'N': 'normal', 'A': 'abnormal',
            'HH': 'critical-high', 'LL': 'critical-low'
        }
        interp_code = interp_map.get(obx_8.upper(), obx_8.lower())
        resource['interpretation'] = [{
            'coding': [{'system': 'http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation',
                        'code': interp_code, 'display': obx_8}]
        }]

    obx_7 = get_field(fields, 7)
    if obx_7 and '-' in obx_7:
        parts = obx_7.split('-')
        try:
            resource['referenceRange'] = [{'low': {'value': float(parts[0])}, 'high': {'value': float(parts[1])}}]
        except ValueError:
            pass

    if effective_date:
        resource['effectiveDateTime'] = effective_date
    if encounter_ref:
        resource['encounter'] = {'reference': f'Encounter/{encounter_ref}'}

    return resource_id, 'Observation', resource

def obr_to_diagnostic_report(fields, msg_id, patient_ref, encounter_ref=None):
    obr_4 = get_field(fields, 4)
    code = get_component(obr_4, 0)
    display = get_component(obr_4, 1)
    code_system_id = get_component(obr_4, 2)

    code_system_map = {'LN': 'http://loinc.org', 'SCT': 'http://snomed.info/sct'}
    code_system = code_system_map.get(code_system_id, f'urn:oid:{code_system_id}' if code_system_id else 'http://loinc.org')

    resource_id = make_id('dr', msg_id, code, patient_ref)

    resource = {
        'resourceType': 'DiagnosticReport',
        'id': resource_id,
        'status': 'final',
        'code': {
            'coding': [{'system': code_system, 'code': code, 'display': display or code}],
            'text': display or code
        },
        'subject': {'reference': f'Patient/{patient_ref}'}
    }

    obr_7 = get_field(fields, 7)
    if len(obr_7) >= 8:
        resource['effectiveDateTime'] = f"{obr_7[:4]}-{obr_7[4:6]}-{obr_7[6:8]}"

    if encounter_ref:
        resource['encounter'] = {'reference': f'Encounter/{encounter_ref}'}

    return resource_id, 'DiagnosticReport', resource

def rxa_to_med_admin(fields, msg_id, patient_ref, encounter_ref=None):
    rxa_5 = get_field(fields, 5)
    code = get_component(rxa_5, 0)
    display = get_component(rxa_5, 1)
    code_system_id = get_component(rxa_5, 2)

    code_system_map = {
        'CVX': 'http://hl7.org/fhir/sid/cvx',
        'NDC': 'http://hl7.org/fhir/sid/ndc',
        'RXN': 'http://www.nlm.nih.gov/research/umls/rxnorm',
        'RXNORM': 'http://www.nlm.nih.gov/research/umls/rxnorm',
    }
    code_system = code_system_map.get(code_system_id, f'urn:oid:{code_system_id}' if code_system_id else 'http://hl7.org/fhir/sid/cvx')

    resource_id = make_id('medadm', msg_id, code, patient_ref)

    resource = {
        'resourceType': 'MedicationAdministration',
        'id': resource_id,
        'status': 'completed',
        'medicationCodeableConcept': {
            'coding': [{'system': code_system, 'code': code, 'display': display or code}],
            'text': display or code
        },
        'subject': {'reference': f'Patient/{patient_ref}'}
    }

    rxa_3 = get_field(fields, 3)
    if len(rxa_3) >= 8:
        admin_date = f"{rxa_3[:4]}-{rxa_3[4:6]}-{rxa_3[6:8]}"
        resource['effectiveDateTime'] = admin_date

    rxa_6 = get_field(fields, 6)
    rxa_7 = get_field(fields, 7)
    if rxa_6:
        try:
            dose = {'value': float(rxa_6)}
            if rxa_7:
                unit_code = get_component(rxa_7, 0)
                unit_display = get_component(rxa_7, 1)
                dose['unit'] = unit_display or unit_code
                dose['system'] = 'http://unitsofmeasure.org'
                dose['code'] = unit_code
            resource['dosage'] = {'dose': dose}
        except ValueError:
            pass

    if encounter_ref:
        resource['context'] = {'reference': f'Encounter/{encounter_ref}'}

    return resource_id, 'MedicationAdministration', resource

def pr1_to_procedure(fields, msg_id, patient_ref, encounter_ref=None):
    pr1_3 = get_field(fields, 3)
    code = get_component(pr1_3, 0)
    display = get_component(pr1_3, 1)
    code_system_id = get_component(pr1_3, 2)

    code_system_map = {
        'CPT': 'http://www.ama-assn.org/go/cpt',
        'C4': 'http://www.ama-assn.org/go/cpt',
        'HCPCS': 'urn:oid:2.16.840.1.113883.6.285',
        'I10P': 'http://www.cms.gov/Medicare/Coding/ICD10',
        'SCT': 'http://snomed.info/sct',
    }
    code_system = code_system_map.get(code_system_id, f'urn:oid:{code_system_id}' if code_system_id else 'http://www.ama-assn.org/go/cpt')

    resource_id = make_id('proc', msg_id, code, patient_ref)

    resource = {
        'resourceType': 'Procedure',
        'id': resource_id,
        'status': 'completed',
        'code': {
            'coding': [{'system': code_system, 'code': code, 'display': display or code}],
            'text': display or code
        },
        'subject': {'reference': f'Patient/{patient_ref}'}
    }

    pr1_5 = get_field(fields, 5)
    if len(pr1_5) >= 8:
        resource['performedDateTime'] = f"{pr1_5[:4]}-{pr1_5[4:6]}-{pr1_5[6:8]}"

    if encounter_ref:
        resource['encounter'] = {'reference': f'Encounter/{encounter_ref}'}

    return resource_id, 'Procedure', resource

def al1_to_allergy(fields, msg_id, patient_ref):
    al1_3 = get_field(fields, 3)
    code = get_component(al1_3, 0)
    display = get_component(al1_3, 1)
    code_system_id = get_component(al1_3, 2)

    code_system_map = {'SCT': 'http://snomed.info/sct', 'RXN': 'http://www.nlm.nih.gov/research/umls/rxnorm'}
    code_system = code_system_map.get(code_system_id, 'http://snomed.info/sct')

    resource_id = make_id('alg', msg_id, code, patient_ref)

    al1_2 = get_field(fields, 2)
    type_map = {'DA': 'allergy', 'FA': 'allergy', 'MA': 'allergy', 'MC': 'allergy', 'EA': 'intolerance', 'LA': 'allergy'}
    allergy_type = type_map.get(al1_2, 'allergy')

    al1_4 = get_field(fields, 4)
    severity_map = {'SV': 'severe', 'MO': 'moderate', 'MI': 'mild', 'U': 'mild'}
    criticality_map = {'SV': 'high', 'MO': 'low', 'MI': 'low', 'U': 'unable-to-assess'}

    resource = {
        'resourceType': 'AllergyIntolerance',
        'id': resource_id,
        'type': allergy_type,
        'patient': {'reference': f'Patient/{patient_ref}'},
        'code': {
            'coding': [{'system': code_system, 'code': code, 'display': display or code}],
            'text': display or code
        },
        'clinicalStatus': {
            'coding': [{'system': 'http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical', 'code': 'active'}]
        }
    }

    if al1_4 in criticality_map:
        resource['criticality'] = criticality_map[al1_4]

    al1_5 = get_field(fields, 5)
    if al1_5:
        resource['reaction'] = [{'manifestation': [{'coding': [{'display': al1_5}]}]}]

    return resource_id, 'AllergyIntolerance', resource

def in1_to_coverage(fields, msg_id, patient_ref):
    in1_3 = get_field(fields, 3)
    plan_id = get_component(in1_3, 0)

    in1_4 = get_field(fields, 4)
    plan_name = get_component(in1_4, 0)

    in1_12 = get_field(fields, 12)
    start_date = ''
    if len(in1_12) >= 8:
        start_date = f"{in1_12[:4]}-{in1_12[4:6]}-{in1_12[6:8]}"

    in1_13 = get_field(fields, 13)
    end_date = ''
    if len(in1_13) >= 8:
        end_date = f"{in1_13[:4]}-{in1_13[4:6]}-{in1_13[6:8]}"

    resource_id = make_id('cov', msg_id, plan_id, patient_ref)

    resource = {
        'resourceType': 'Coverage',
        'id': resource_id,
        'status': 'active',
        'beneficiary': {'reference': f'Patient/{patient_ref}'},
        'payor': [{'display': plan_name or plan_id}]
    }

    if start_date or end_date:
        period = {}
        if start_date: period['start'] = start_date
        if end_date: period['end'] = end_date
        resource['period'] = period

    in1_2 = get_field(fields, 2)
    if in1_2:
        type_map = {'1': 'pay', '2': 'pay', '3': 'pay'}
        resource['type'] = {
            'coding': [{'system': 'http://terminology.hl7.org/CodeSystem/coverage-type',
                        'code': type_map.get(in1_2, 'pay')}]
        }

    return resource_id, 'Coverage', resource

def nk1_to_related_person(fields, msg_id, patient_ref):
    nk1_2 = get_field(fields, 2)
    family = get_component(nk1_2, 0)
    given = get_component(nk1_2, 1)

    nk1_3 = get_field(fields, 3)
    rel_code = get_component(nk1_3, 0)
    rel_display = get_component(nk1_3, 1)

    rel_code_map = {
        'MTH': ('MTH', 'Mother'), 'FTH': ('FTH', 'Father'),
        'SPO': ('SPS', 'Spouse'), 'CHD': ('CHILD', 'Child'),
        'SIB': ('SIB', 'Sibling'), 'GRD': ('GUARD', 'Guardian'),
        'EMC': ('ECON', 'Emergency Contact'),
    }
    fhir_code, fhir_display = rel_code_map.get(rel_code, (rel_code, rel_display or rel_code))

    resource_id = make_id('rp', msg_id, rel_code, patient_ref)

    resource = {
        'resourceType': 'RelatedPerson',
        'id': resource_id,
        'patient': {'reference': f'Patient/{patient_ref}'},
        'relationship': [{
            'coding': [{'system': 'http://terminology.hl7.org/CodeSystem/v3-RoleCode',
                        'code': fhir_code, 'display': fhir_display}]
        }]
    }

    name = {}
    if family: name['family'] = family
    if given: name['given'] = [given]
    if name:
        resource['name'] = [name]

    return resource_id, 'RelatedPerson', resource

def convert_message_to_fhir(raw_message, msg_id):
    msg_type, trigger, segments = parse_hl7v2_message(raw_message)
    if not segments:
        return []

    resources = []
    patient_ref = None
    encounter_ref = None

    for seg_type, fields in segments:
        if seg_type == 'PID':
            rid, rtype, res = pid_to_patient(fields, msg_id)
            patient_ref = rid
            resources.append((rid, rtype, res))

    for seg_type, fields in segments:
        if seg_type == 'PV1' and patient_ref:
            rid, rtype, res = pv1_to_encounter(fields, msg_id, patient_ref)
            encounter_ref = rid
            resources.append((rid, rtype, res))
            break

    for seg_type, fields in segments:
        if seg_type == 'MSH' or seg_type == 'EVN' or seg_type == 'PID' or seg_type == 'PV1':
            continue
        if not patient_ref:
            continue

        if seg_type == 'DG1':
            rid, rtype, res = dg1_to_condition(fields, msg_id, patient_ref, encounter_ref)
            resources.append((rid, rtype, res))
        elif seg_type == 'OBX':
            rid, rtype, res = obx_to_observation(fields, msg_id, patient_ref, encounter_ref)
            resources.append((rid, rtype, res))
        elif seg_type == 'OBR':
            rid, rtype, res = obr_to_diagnostic_report(fields, msg_id, patient_ref, encounter_ref)
            resources.append((rid, rtype, res))
        elif seg_type == 'RXA':
            rid, rtype, res = rxa_to_med_admin(fields, msg_id, patient_ref, encounter_ref)
            resources.append((rid, rtype, res))
        elif seg_type == 'PR1':
            rid, rtype, res = pr1_to_procedure(fields, msg_id, patient_ref, encounter_ref)
            resources.append((rid, rtype, res))
        elif seg_type == 'AL1':
            rid, rtype, res = al1_to_allergy(fields, msg_id, patient_ref)
            resources.append((rid, rtype, res))
        elif seg_type == 'IN1':
            rid, rtype, res = in1_to_coverage(fields, msg_id, patient_ref)
            resources.append((rid, rtype, res))
        elif seg_type == 'NK1':
            rid, rtype, res = nk1_to_related_person(fields, msg_id, patient_ref)
            resources.append((rid, rtype, res))

    return resources

def run(session, source_table: str, message_column: str, message_id_column: str) -> str:
    session.sql("CREATE SCHEMA IF NOT EXISTS app_state").collect()
    session.sql("""
        CREATE TABLE IF NOT EXISTS app_state.fhir_resources (
            resource_id     VARCHAR(256),
            resource_type   VARCHAR(100),
            bundle_id       VARCHAR(256),
            resource_json   VARIANT,
            parsed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

    session.sql("TRUNCATE TABLE IF EXISTS app_state.fhir_resources").collect()

    rows = session.sql(f"""
        SELECT {message_id_column}::VARCHAR AS msg_id, {message_column}::VARCHAR AS raw_msg
        FROM {source_table}
        WHERE {message_column} IS NOT NULL
    """).collect()

    total = 0
    errors = 0
    msg_count = len(rows)
    all_values = []

    for row in rows:
        msg_id = row['MSG_ID'] or f'HL7-{total}'
        raw_msg = row['RAW_MSG']
        if not raw_msg or not raw_msg.strip():
            continue

        try:
            resources = convert_message_to_fhir(raw_msg.strip(), msg_id)
            for rid, rtype, res in resources:
                all_values.append((str(rid), rtype, str(msg_id), res))
                total += 1
        except Exception as e:
            err_msg = str(e)[:500]
            all_values.append(('ERROR', 'ParseError', str(msg_id), {"error": err_msg}))
            errors += 1

    if all_values:
        batch_size = 500
        for i in range(0, len(all_values), batch_size):
            batch = all_values[i:i+batch_size]
            union_parts = []
            for rid, rtype, bid, rjson in batch:
                rj = json.dumps(rjson).replace("'", "''")
                rid_safe = rid.replace("'", "''")
                bid_safe = bid.replace("'", "''")
                union_parts.append(
                    f"SELECT '{rid_safe}', '{rtype}', '{bid_safe}', PARSE_JSON('{rj}')"
                )
            union_sql = " UNION ALL ".join(union_parts)
            session.sql(f"""
                INSERT INTO app_state.fhir_resources
                    (resource_id, resource_type, bundle_id, resource_json)
                {union_sql}
            """).collect()

    msg = f"Converted {total} FHIR resources from {msg_count} HL7v2 messages in {source_table}"
    if errors > 0:
        msg += f" ({errors} parse errors)"
    return msg
$$;
GRANT USAGE ON PROCEDURE core.parse_hl7v2_to_fhir(VARCHAR, VARCHAR, VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Person Mapper — FHIR Patient → OMOP person (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_persons(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.person (
            person_id                   INTEGER,
            gender_concept_id           INTEGER,
            year_of_birth               INTEGER,
            month_of_birth              INTEGER,
            day_of_birth                INTEGER,
            birth_datetime              TIMESTAMP_NTZ,
            race_concept_id             INTEGER,
            ethnicity_concept_id        INTEGER,
            location_id                 INTEGER,
            provider_id                 INTEGER,
            care_site_id                INTEGER,
            person_source_value         VARCHAR(256),
            gender_source_value         VARCHAR(50),
            gender_source_concept_id    INTEGER,
            race_source_value           VARCHAR(50),
            race_source_concept_id      INTEGER,
            ethnicity_source_value      VARCHAR(50),
            ethnicity_source_concept_id INTEGER
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.person
        WITH patients AS (
            SELECT
                resource_json:id::VARCHAR AS patient_id,
                resource_json:gender::VARCHAR AS gender,
                resource_json:birthDate::DATE AS birth_date,
                resource_json:birthDate::TIMESTAMP_NTZ AS birth_datetime,
                resource_json AS rj
            FROM app_state.fhir_resources
            WHERE resource_type = 'Patient'
        ),
        race_ext AS (
            SELECT p.patient_id,
                   e.value:valueCoding:code::VARCHAR AS race_code,
                   e.value:valueCoding:display::VARCHAR AS race_display
            FROM patients p,
                LATERAL FLATTEN(input => p.rj:extension, OUTER => TRUE) e
            WHERE e.value:url::VARCHAR LIKE '%us-core-race'
        ),
        eth_ext AS (
            SELECT p.patient_id,
                   e.value:valueCoding:code::VARCHAR AS eth_code,
                   e.value:valueCoding:display::VARCHAR AS eth_display
            FROM patients p,
                LATERAL FLATTEN(input => p.rj:extension, OUTER => TRUE) e
            WHERE e.value:url::VARCHAR LIKE '%us-core-ethnicity'
        )
        SELECT
            ABS(HASH(p.patient_id)) % 2147483647    AS person_id,
            COALESCE(dg.omop_concept_id, 0)          AS gender_concept_id,
            YEAR(p.birth_date)                        AS year_of_birth,
            MONTH(p.birth_date)                       AS month_of_birth,
            DAY(p.birth_date)                         AS day_of_birth,
            p.birth_datetime,
            COALESCE(dr.omop_concept_id, 0)           AS race_concept_id,
            COALESCE(de.omop_concept_id, 0)           AS ethnicity_concept_id,
            NULL AS location_id,
            NULL AS provider_id,
            NULL AS care_site_id,
            p.patient_id                              AS person_source_value,
            p.gender                                  AS gender_source_value,
            0 AS gender_source_concept_id,
            r.race_display                            AS race_source_value,
            0 AS race_source_concept_id,
            e.eth_display                             AS ethnicity_source_value,
            0 AS ethnicity_source_concept_id
        FROM patients p
        LEFT JOIN terminology.demographic_to_omop dg
            ON dg.source_code = LOWER(p.gender) AND dg.category = 'gender'
        LEFT JOIN race_ext r ON r.patient_id = p.patient_id
        LEFT JOIN terminology.demographic_to_omop dr
            ON dr.source_code = r.race_code AND dr.category = 'race'
        LEFT JOIN eth_ext e ON e.patient_id = p.patient_id
        LEFT JOIN terminology.demographic_to_omop de
            ON de.source_code = e.eth_code AND de.category = 'ethnicity'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.person").collect()[0]['CNT']
    return f"Mapped {count} persons to {output_schema}.person"
$$;
GRANT USAGE ON PROCEDURE core.map_persons(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Condition Mapper — FHIR Condition → OMOP condition_occurrence (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_conditions(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.condition_occurrence (
            condition_occurrence_id     INTEGER,
            person_id                   INTEGER,
            condition_concept_id        INTEGER,
            condition_start_date        DATE,
            condition_start_datetime    TIMESTAMP_NTZ,
            condition_end_date          DATE,
            condition_end_datetime      TIMESTAMP_NTZ,
            condition_type_concept_id   INTEGER DEFAULT 32817,
            condition_status_concept_id INTEGER DEFAULT 0,
            stop_reason                 VARCHAR(256),
            provider_id                 INTEGER,
            visit_occurrence_id         INTEGER,
            condition_source_value      VARCHAR(256),
            condition_source_concept_id INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.condition_occurrence
        WITH cond_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS cond_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:onsetDateTime::DATE, r.resource_json:onsetPeriod:start::DATE, r.resource_json:recordedDate::DATE) AS start_date,
                COALESCE(r.resource_json:onsetDateTime::TIMESTAMP_NTZ, r.resource_json:onsetPeriod:start::TIMESTAMP_NTZ) AS start_dt,
                r.resource_json:abatementDateTime::DATE AS end_date,
                r.resource_json:abatementDateTime::TIMESTAMP_NTZ AS end_dt,
                cc.value:code::VARCHAR AS source_code
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) cc
            WHERE r.resource_type = 'Condition'
        )
        SELECT
            ABS(HASH(c.cond_id)) % 2147483647 AS condition_occurrence_id,
            ABS(HASH(c.patient_ref)) % 2147483647 AS person_id,
            COALESCE(sm.omop_concept_id, 0) AS condition_concept_id,
            c.start_date AS condition_start_date,
            c.start_dt AS condition_start_datetime,
            c.end_date AS condition_end_date,
            c.end_dt AS condition_end_datetime,
            32817 AS condition_type_concept_id,
            0 AS condition_status_concept_id,
            NULL AS stop_reason,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            c.source_code AS condition_source_value,
            0 AS condition_source_concept_id
        FROM cond_flat c
        LEFT JOIN terminology.snomed_to_omop sm
            ON sm.snomed_code = c.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.condition_occurrence").collect()[0]['CNT']
    return f"Mapped {count} conditions to {output_schema}.condition_occurrence"
$$;
GRANT USAGE ON PROCEDURE core.map_conditions(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Measurement Mapper — FHIR Observation (numeric) → OMOP measurement (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_measurements(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.measurement (
            measurement_id              INTEGER,
            person_id                   INTEGER,
            measurement_concept_id      INTEGER,
            measurement_date            DATE,
            measurement_datetime        TIMESTAMP_NTZ,
            measurement_type_concept_id INTEGER DEFAULT 32817,
            operator_concept_id         INTEGER DEFAULT 0,
            value_as_number             FLOAT,
            value_as_concept_id         INTEGER DEFAULT 0,
            unit_concept_id             INTEGER DEFAULT 0,
            range_low                   FLOAT,
            range_high                  FLOAT,
            provider_id                 INTEGER,
            visit_occurrence_id         INTEGER,
            measurement_source_value    VARCHAR(256),
            measurement_source_concept_id INTEGER DEFAULT 0,
            unit_source_value           VARCHAR(50),
            value_source_value          VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.measurement
        WITH obs_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS obs_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:effectiveDateTime::DATE, r.resource_json:issued::DATE) AS meas_date,
                COALESCE(r.resource_json:effectiveDateTime::TIMESTAMP_NTZ, r.resource_json:issued::TIMESTAMP_NTZ) AS meas_dt,
                r.resource_json:valueQuantity:value::FLOAT AS val_num,
                r.resource_json:referenceRange[0]:low:value::FLOAT AS range_low,
                r.resource_json:referenceRange[0]:high:value::FLOAT AS range_high,
                oc.value:code::VARCHAR AS source_code,
                r.resource_json:valueQuantity:unit::VARCHAR AS unit_src,
                r.resource_json:valueQuantity:value::VARCHAR AS val_src
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) oc
            WHERE r.resource_type = 'Observation'
                AND r.resource_json:valueQuantity IS NOT NULL
        )
        SELECT
            ABS(HASH(o.obs_id)) % 2147483647 AS measurement_id,
            ABS(HASH(o.patient_ref)) % 2147483647 AS person_id,
            COALESCE(lm.omop_concept_id, 0) AS measurement_concept_id,
            o.meas_date AS measurement_date,
            o.meas_dt AS measurement_datetime,
            32817 AS measurement_type_concept_id,
            0 AS operator_concept_id,
            o.val_num AS value_as_number,
            0 AS value_as_concept_id,
            0 AS unit_concept_id,
            o.range_low, o.range_high,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            o.source_code AS measurement_source_value,
            0 AS measurement_source_concept_id,
            o.unit_src AS unit_source_value,
            o.val_src AS value_source_value
        FROM obs_flat o
        LEFT JOIN terminology.loinc_to_omop lm
            ON lm.loinc_code = o.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.measurement").collect()[0]['CNT']
    return f"Mapped {count} measurements to {output_schema}.measurement"
$$;
GRANT USAGE ON PROCEDURE core.map_measurements(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Visit Mapper — FHIR Encounter → OMOP visit_occurrence
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_visits(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.visit_occurrence (
            visit_occurrence_id         INTEGER,
            person_id                   INTEGER,
            visit_concept_id            INTEGER,
            visit_start_date            DATE,
            visit_start_datetime        TIMESTAMP_NTZ,
            visit_end_date              DATE,
            visit_end_datetime          TIMESTAMP_NTZ,
            visit_type_concept_id       INTEGER DEFAULT 32817,
            provider_id                 INTEGER,
            care_site_id                INTEGER,
            visit_source_value          VARCHAR(256),
            visit_source_concept_id     INTEGER DEFAULT 0,
            admitted_from_concept_id    INTEGER DEFAULT 0,
            discharged_to_concept_id    INTEGER DEFAULT 0
        )
    """).collect()

    visit_type_map = {
        'AMB': 9202, 'IMP': 9201, 'EMER': 9203, 'HH': 581476,
        'FLD': 38004193, 'VR': 5083, 'SS': 9202,
    }
    cases = " ".join([f"WHEN '{k}' THEN {v}" for k, v in visit_type_map.items()])

    session.sql(f"""
        INSERT INTO {output_schema}.visit_occurrence
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS visit_occurrence_id,
            ABS(HASH(SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1))) % 2147483647 AS person_id,
            CASE r.resource_json:class:code::VARCHAR {cases} ELSE 0 END AS visit_concept_id,
            r.resource_json:period:start::DATE AS visit_start_date,
            r.resource_json:period:start::TIMESTAMP_NTZ AS visit_start_datetime,
            COALESCE(r.resource_json:period:end::DATE, r.resource_json:period:start::DATE) AS visit_end_date,
            r.resource_json:period:end::TIMESTAMP_NTZ AS visit_end_datetime,
            32817 AS visit_type_concept_id,
            NULL AS provider_id,
            NULL AS care_site_id,
            r.resource_json:class:code::VARCHAR AS visit_source_value,
            0 AS visit_source_concept_id,
            0 AS admitted_from_concept_id,
            0 AS discharged_to_concept_id
        FROM app_state.fhir_resources r
        WHERE r.resource_type = 'Encounter'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.visit_occurrence").collect()[0]['CNT']
    return f"Mapped {count} visits to {output_schema}.visit_occurrence"
$$;
GRANT USAGE ON PROCEDURE core.map_visits(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Drug Exposure Mapper — FHIR MedicationRequest → OMOP drug_exposure (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_drug_exposures(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.drug_exposure (
            drug_exposure_id            INTEGER,
            person_id                   INTEGER,
            drug_concept_id             INTEGER,
            drug_exposure_start_date    DATE,
            drug_exposure_start_datetime TIMESTAMP_NTZ,
            drug_exposure_end_date      DATE,
            drug_exposure_end_datetime  TIMESTAMP_NTZ,
            verbatim_end_date           DATE,
            drug_type_concept_id        INTEGER DEFAULT 32817,
            stop_reason                 VARCHAR(256),
            refills                     INTEGER,
            quantity                    FLOAT,
            days_supply                 INTEGER,
            sig                         VARCHAR(1024),
            route_concept_id            INTEGER DEFAULT 0,
            lot_number                  VARCHAR(50),
            provider_id                 INTEGER,
            visit_occurrence_id         INTEGER,
            drug_source_value           VARCHAR(256),
            drug_source_concept_id      INTEGER DEFAULT 0,
            route_source_value          VARCHAR(256),
            dose_unit_source_value      VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.drug_exposure
        WITH med_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS med_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:authoredOn::DATE, r.resource_json:dispenseRequest:validityPeriod:start::DATE) AS start_date,
                r.resource_json:authoredOn::TIMESTAMP_NTZ AS start_dt,
                r.resource_json:dispenseRequest:validityPeriod:end::DATE AS end_date,
                r.resource_json:dispenseRequest:numberOfRepeatsAllowed::INTEGER AS refills,
                r.resource_json:dispenseRequest:quantity:value::FLOAT AS quantity,
                r.resource_json:dispenseRequest:expectedSupplyDuration:value::INTEGER AS days_supply,
                r.resource_json:dosageInstruction[0]:text::VARCHAR AS sig,
                mc.value:code::VARCHAR AS source_code,
                r.resource_json:dosageInstruction[0]:route:coding[0]:display::VARCHAR AS route_src,
                r.resource_json:dosageInstruction[0]:doseAndRate[0]:doseQuantity:unit::VARCHAR AS dose_unit_src
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:medicationCodeableConcept:coding, OUTER => TRUE) mc
            WHERE r.resource_type = 'MedicationRequest'
        )
        SELECT
            ABS(HASH(m.med_id)) % 2147483647 AS drug_exposure_id,
            ABS(HASH(m.patient_ref)) % 2147483647 AS person_id,
            COALESCE(rx.omop_concept_id, 0) AS drug_concept_id,
            m.start_date AS drug_exposure_start_date,
            m.start_dt AS drug_exposure_start_datetime,
            m.end_date AS drug_exposure_end_date,
            NULL::TIMESTAMP_NTZ AS drug_exposure_end_datetime,
            NULL::DATE AS verbatim_end_date,
            32817 AS drug_type_concept_id,
            NULL::VARCHAR(256) AS stop_reason,
            m.refills, m.quantity, m.days_supply, m.sig,
            0 AS route_concept_id,
            NULL::VARCHAR(50) AS lot_number,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            m.source_code AS drug_source_value,
            0 AS drug_source_concept_id,
            m.route_src AS route_source_value,
            m.dose_unit_src AS dose_unit_source_value
        FROM med_flat m
        LEFT JOIN terminology.rxnorm_to_omop rx
            ON rx.rxnorm_code = m.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.drug_exposure").collect()[0]['CNT']
    return f"Mapped {count} drug exposures to {output_schema}.drug_exposure"
$$;
GRANT USAGE ON PROCEDURE core.map_drug_exposures(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Procedure Mapper — FHIR Procedure → OMOP procedure_occurrence (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_procedures(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.procedure_occurrence (
            procedure_occurrence_id      INTEGER,
            person_id                    INTEGER,
            procedure_concept_id         INTEGER,
            procedure_date               DATE,
            procedure_datetime           TIMESTAMP_NTZ,
            procedure_end_date           DATE,
            procedure_type_concept_id    INTEGER DEFAULT 32817,
            provider_id                  INTEGER,
            visit_occurrence_id          INTEGER,
            procedure_source_value       VARCHAR(256),
            procedure_source_concept_id  INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.procedure_occurrence
        WITH proc_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS proc_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:performedDateTime::DATE, r.resource_json:performedPeriod:start::DATE) AS proc_date,
                COALESCE(r.resource_json:performedDateTime::TIMESTAMP_NTZ, r.resource_json:performedPeriod:start::TIMESTAMP_NTZ) AS proc_dt,
                r.resource_json:performedPeriod:end::DATE AS proc_end_date,
                pc.value:code::VARCHAR AS source_code,
                pc.value:system::VARCHAR AS code_system
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) pc
            WHERE r.resource_type = 'Procedure'
        )
        SELECT
            ABS(HASH(pf.proc_id)) % 2147483647 AS procedure_occurrence_id,
            ABS(HASH(pf.patient_ref)) % 2147483647 AS person_id,
            COALESCE(sm.omop_concept_id, cpt.omop_concept_id, hc.omop_concept_id, 0) AS procedure_concept_id,
            pf.proc_date AS procedure_date,
            pf.proc_dt AS procedure_datetime,
            pf.proc_end_date AS procedure_end_date,
            32817 AS procedure_type_concept_id,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            pf.source_code AS procedure_source_value,
            0 AS procedure_source_concept_id
        FROM proc_flat pf
        LEFT JOIN terminology.snomed_to_omop sm
            ON sm.snomed_code = pf.source_code AND pf.code_system LIKE '%snomed%'
        LEFT JOIN terminology.cpt_to_omop cpt
            ON cpt.cpt_code = pf.source_code AND pf.code_system LIKE '%cpt%'
        LEFT JOIN terminology.hcpcs_to_omop hc
            ON hc.hcpcs_code = pf.source_code AND pf.code_system LIKE '%hcpcs%'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.procedure_occurrence").collect()[0]['CNT']
    return f"Mapped {count} procedures to {output_schema}.procedure_occurrence"
$$;
GRANT USAGE ON PROCEDURE core.map_procedures(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Observation (Qualitative) Mapper — FHIR Observation → OMOP observation (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_observations_qual(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.observation (
            observation_id                INTEGER,
            person_id                     INTEGER,
            observation_concept_id        INTEGER,
            observation_date              DATE,
            observation_datetime          TIMESTAMP_NTZ,
            observation_type_concept_id   INTEGER DEFAULT 32817,
            value_as_string               VARCHAR(1024),
            value_as_concept_id           INTEGER DEFAULT 0,
            observation_source_value      VARCHAR(256),
            observation_source_concept_id INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.observation
        WITH obs_qual AS (
            SELECT
                r.resource_json:id::VARCHAR AS obs_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:effectiveDateTime::DATE, r.resource_json:issued::DATE) AS obs_date,
                COALESCE(r.resource_json:effectiveDateTime::TIMESTAMP_NTZ, r.resource_json:issued::TIMESTAMP_NTZ) AS obs_dt,
                COALESCE(r.resource_json:valueCodeableConcept:text::VARCHAR, r.resource_json:valueCodeableConcept:coding[0]:display::VARCHAR, r.resource_json:valueString::VARCHAR) AS val_str,
                oc.value:code::VARCHAR AS source_code
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) oc
            WHERE r.resource_type = 'Observation'
                AND r.resource_json:valueQuantity IS NULL
                AND (r.resource_json:valueCodeableConcept IS NOT NULL OR r.resource_json:valueString IS NOT NULL)
        )
        SELECT
            ABS(HASH(q.obs_id)) % 2147483647 AS observation_id,
            ABS(HASH(q.patient_ref)) % 2147483647 AS person_id,
            COALESCE(lm.omop_concept_id, 0) AS observation_concept_id,
            q.obs_date AS observation_date,
            q.obs_dt AS observation_datetime,
            32817 AS observation_type_concept_id,
            q.val_str AS value_as_string,
            0 AS value_as_concept_id,
            q.source_code AS observation_source_value,
            0 AS observation_source_concept_id
        FROM obs_qual q
        LEFT JOIN terminology.loinc_to_omop lm
            ON lm.loinc_code = q.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.observation").collect()[0]['CNT']
    return f"Mapped {count} qualitative observations to {output_schema}.observation"
$$;
GRANT USAGE ON PROCEDURE core.map_observations_qual(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Death Mapper — FHIR Patient (deceasedDateTime/deceasedBoolean) → OMOP death
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_death(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.death (
            person_id                INTEGER,
            death_date               DATE,
            death_datetime           TIMESTAMP_NTZ,
            death_type_concept_id    INTEGER DEFAULT 32817,
            cause_concept_id         INTEGER DEFAULT 0,
            cause_source_value       VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.death
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS person_id,
            r.resource_json:deceasedDateTime::DATE AS death_date,
            r.resource_json:deceasedDateTime::TIMESTAMP_NTZ AS death_datetime,
            32817 AS death_type_concept_id,
            0 AS cause_concept_id,
            NULL AS cause_source_value
        FROM app_state.fhir_resources r
        WHERE r.resource_type = 'Patient'
            AND (r.resource_json:deceasedDateTime IS NOT NULL
                 OR r.resource_json:deceasedBoolean::BOOLEAN = TRUE)
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.death").collect()[0]['CNT']
    return f"Mapped {count} death records to {output_schema}.death"
$$;
GRANT USAGE ON PROCEDURE core.map_death(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Immunization Mapper — FHIR Immunization → OMOP drug_exposure (appends)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_immunizations(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.drug_exposure (
            drug_exposure_id            INTEGER,
            person_id                   INTEGER,
            drug_concept_id             INTEGER,
            drug_exposure_start_date    DATE,
            drug_exposure_start_datetime TIMESTAMP_NTZ,
            drug_exposure_end_date      DATE,
            drug_exposure_end_datetime  TIMESTAMP_NTZ,
            verbatim_end_date           DATE,
            drug_type_concept_id        INTEGER DEFAULT 32817,
            stop_reason                 VARCHAR(256),
            refills                     INTEGER,
            quantity                    FLOAT,
            days_supply                 INTEGER,
            sig                         VARCHAR(1024),
            route_concept_id            INTEGER DEFAULT 0,
            lot_number                  VARCHAR(50),
            provider_id                 INTEGER,
            visit_occurrence_id         INTEGER,
            drug_source_value           VARCHAR(256),
            drug_source_concept_id      INTEGER DEFAULT 0,
            route_source_value          VARCHAR(256),
            dose_unit_source_value      VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.drug_exposure
        WITH imm_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS imm_id,
                SPLIT_PART(r.resource_json:patient:reference::VARCHAR, '/', -1) AS patient_ref,
                r.resource_json:occurrenceDateTime::DATE AS occ_date,
                r.resource_json:occurrenceDateTime::TIMESTAMP_NTZ AS occ_dt,
                vc.value:code::VARCHAR AS source_code,
                r.resource_json:lotNumber::VARCHAR AS lot_num,
                r.resource_json:route:coding[0]:display::VARCHAR AS route_src,
                r.resource_json:doseQuantity:value::FLOAT AS dose_qty,
                r.resource_json:doseQuantity:unit::VARCHAR AS dose_unit
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:vaccineCode:coding, OUTER => TRUE) vc
            WHERE r.resource_type = 'Immunization'
        )
        SELECT
            ABS(HASH(i.imm_id)) % 2147483647 AS drug_exposure_id,
            ABS(HASH(i.patient_ref)) % 2147483647 AS person_id,
            COALESCE(cv.concept_id, 0) AS drug_concept_id,
            i.occ_date AS drug_exposure_start_date,
            i.occ_dt AS drug_exposure_start_datetime,
            i.occ_date AS drug_exposure_end_date,
            NULL::TIMESTAMP_NTZ AS drug_exposure_end_datetime,
            NULL::DATE AS verbatim_end_date,
            32817 AS drug_type_concept_id,
            NULL::VARCHAR(256) AS stop_reason,
            NULL::INTEGER AS refills,
            i.dose_qty AS quantity,
            1 AS days_supply,
            NULL::VARCHAR(1024) AS sig,
            0 AS route_concept_id,
            i.lot_num AS lot_number,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            i.source_code AS drug_source_value,
            0 AS drug_source_concept_id,
            i.route_src AS route_source_value,
            i.dose_unit AS dose_unit_source_value
        FROM imm_flat i
        LEFT JOIN terminology.concept cv
            ON cv.concept_code = i.source_code AND cv.vocabulary_id = 'CVX'
    """).collect()

    count = session.sql(f"""
        SELECT COUNT(*) AS cnt FROM {output_schema}.drug_exposure
        WHERE drug_source_value IN (
            SELECT resource_json:vaccineCode:coding[0]:code::VARCHAR
            FROM app_state.fhir_resources WHERE resource_type = 'Immunization'
        )
    """).collect()[0]['CNT']
    return f"Appended {count} immunizations to {output_schema}.drug_exposure"
$$;
GRANT USAGE ON PROCEDURE core.map_immunizations(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- MedicationAdministration Mapper — FHIR MedicationAdministration → OMOP drug_exposure (appends)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_med_administrations(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.drug_exposure (
            drug_exposure_id            INTEGER,
            person_id                   INTEGER,
            drug_concept_id             INTEGER,
            drug_exposure_start_date    DATE,
            drug_exposure_start_datetime TIMESTAMP_NTZ,
            drug_exposure_end_date      DATE,
            drug_exposure_end_datetime  TIMESTAMP_NTZ,
            verbatim_end_date           DATE,
            drug_type_concept_id        INTEGER DEFAULT 32817,
            stop_reason                 VARCHAR(256),
            refills                     INTEGER,
            quantity                    FLOAT,
            days_supply                 INTEGER,
            sig                         VARCHAR(1024),
            route_concept_id            INTEGER DEFAULT 0,
            lot_number                  VARCHAR(50),
            provider_id                 INTEGER,
            visit_occurrence_id         INTEGER,
            drug_source_value           VARCHAR(256),
            drug_source_concept_id      INTEGER DEFAULT 0,
            route_source_value          VARCHAR(256),
            dose_unit_source_value      VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.drug_exposure
        WITH admin_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS admin_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:effectiveDateTime::DATE, r.resource_json:effectivePeriod:start::DATE) AS start_date,
                COALESCE(r.resource_json:effectiveDateTime::TIMESTAMP_NTZ, r.resource_json:effectivePeriod:start::TIMESTAMP_NTZ) AS start_dt,
                r.resource_json:effectivePeriod:end::DATE AS end_date,
                r.resource_json:effectivePeriod:end::TIMESTAMP_NTZ AS end_dt,
                mc.value:code::VARCHAR AS source_code,
                r.resource_json:dosage:dose:value::FLOAT AS dose_qty,
                r.resource_json:dosage:dose:unit::VARCHAR AS dose_unit,
                r.resource_json:dosage:route:coding[0]:display::VARCHAR AS route_src
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:medicationCodeableConcept:coding, OUTER => TRUE) mc
            WHERE r.resource_type = 'MedicationAdministration'
        )
        SELECT
            ABS(HASH(a.admin_id)) % 2147483647 AS drug_exposure_id,
            ABS(HASH(a.patient_ref)) % 2147483647 AS person_id,
            COALESCE(rx.omop_concept_id, 0) AS drug_concept_id,
            a.start_date AS drug_exposure_start_date,
            a.start_dt AS drug_exposure_start_datetime,
            COALESCE(a.end_date, a.start_date) AS drug_exposure_end_date,
            a.end_dt AS drug_exposure_end_datetime,
            NULL::DATE AS verbatim_end_date,
            32817 AS drug_type_concept_id,
            NULL::VARCHAR(256) AS stop_reason,
            NULL::INTEGER AS refills,
            a.dose_qty AS quantity,
            NULL::INTEGER AS days_supply,
            NULL::VARCHAR(1024) AS sig,
            0 AS route_concept_id,
            NULL::VARCHAR(50) AS lot_number,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            a.source_code AS drug_source_value,
            0 AS drug_source_concept_id,
            a.route_src AS route_source_value,
            a.dose_unit AS dose_unit_source_value
        FROM admin_flat a
        LEFT JOIN terminology.rxnorm_to_omop rx
            ON rx.rxnorm_code = a.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.drug_exposure").collect()[0]['CNT']
    return f"Mapped MedicationAdministration records; total drug_exposure now {count}"
$$;
GRANT USAGE ON PROCEDURE core.map_med_administrations(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- AllergyIntolerance Mapper — FHIR AllergyIntolerance → OMOP observation (appends)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_allergies(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.observation (
            observation_id                INTEGER,
            person_id                     INTEGER,
            observation_concept_id        INTEGER,
            observation_date              DATE,
            observation_datetime          TIMESTAMP_NTZ,
            observation_type_concept_id   INTEGER DEFAULT 32817,
            value_as_string               VARCHAR(1024),
            value_as_concept_id           INTEGER DEFAULT 0,
            observation_source_value      VARCHAR(256),
            observation_source_concept_id INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.observation
        WITH allergy_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS allergy_id,
                SPLIT_PART(r.resource_json:patient:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:onsetDateTime::DATE, r.resource_json:recordedDate::DATE) AS obs_date,
                COALESCE(r.resource_json:onsetDateTime::TIMESTAMP_NTZ, r.resource_json:recordedDate::TIMESTAMP_NTZ) AS obs_dt,
                ac.value:code::VARCHAR AS source_code,
                COALESCE(ac.value:display::VARCHAR, r.resource_json:code:text::VARCHAR) AS allergy_desc,
                r.resource_json:clinicalStatus:coding[0]:code::VARCHAR AS clinical_status,
                r.resource_json:type::VARCHAR AS allergy_type,
                r.resource_json:category[0]::VARCHAR AS category
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) ac
            WHERE r.resource_type = 'AllergyIntolerance'
        )
        SELECT
            ABS(HASH(a.allergy_id)) % 2147483647 AS observation_id,
            ABS(HASH(a.patient_ref)) % 2147483647 AS person_id,
            439224 AS observation_concept_id,
            a.obs_date AS observation_date,
            a.obs_dt AS observation_datetime,
            32817 AS observation_type_concept_id,
            CONCAT(COALESCE(a.allergy_desc, ''), ' [', COALESCE(a.allergy_type, ''), '/', COALESCE(a.category, ''), '] ', COALESCE(a.clinical_status, '')) AS value_as_string,
            0 AS value_as_concept_id,
            a.source_code AS observation_source_value,
            0 AS observation_source_concept_id
        FROM allergy_flat a
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.observation").collect()[0]['CNT']
    return f"Mapped allergies; total observation now {count}"
$$;
GRANT USAGE ON PROCEDURE core.map_allergies(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Device Mapper — FHIR Device → OMOP device_exposure (new table)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_devices(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.device_exposure (
            device_exposure_id          INTEGER,
            person_id                   INTEGER,
            device_concept_id           INTEGER,
            device_exposure_start_date  DATE,
            device_exposure_start_datetime TIMESTAMP_NTZ,
            device_exposure_end_date    DATE,
            device_exposure_end_datetime TIMESTAMP_NTZ,
            device_type_concept_id      INTEGER DEFAULT 32817,
            unique_device_id            VARCHAR(256),
            production_id               VARCHAR(256),
            quantity                    INTEGER DEFAULT 1,
            provider_id                 INTEGER,
            visit_occurrence_id         INTEGER,
            device_source_value         VARCHAR(256),
            device_source_concept_id    INTEGER DEFAULT 0,
            unit_concept_id             INTEGER DEFAULT 0,
            unit_source_value           VARCHAR(50),
            unit_source_concept_id      INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.device_exposure
        WITH dev_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS device_id,
                SPLIT_PART(r.resource_json:patient:reference::VARCHAR, '/', -1) AS patient_ref,
                r.resource_json:manufactureDate::DATE AS start_date,
                r.resource_json:manufactureDate::TIMESTAMP_NTZ AS start_dt,
                r.resource_json:expirationDate::DATE AS end_date,
                dc.value:code::VARCHAR AS source_code,
                COALESCE(dc.value:display::VARCHAR, r.resource_json:type:text::VARCHAR) AS device_desc,
                r.resource_json:udiCarrier[0]:deviceIdentifier::VARCHAR AS udi,
                r.resource_json:serialNumber::VARCHAR AS serial_num
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:type:coding, OUTER => TRUE) dc
            WHERE r.resource_type = 'Device'
        )
        SELECT
            ABS(HASH(d.device_id)) % 2147483647 AS device_exposure_id,
            ABS(HASH(d.patient_ref)) % 2147483647 AS person_id,
            COALESCE(sm.omop_concept_id, 0) AS device_concept_id,
            d.start_date AS device_exposure_start_date,
            d.start_dt AS device_exposure_start_datetime,
            d.end_date AS device_exposure_end_date,
            NULL::TIMESTAMP_NTZ AS device_exposure_end_datetime,
            32817 AS device_type_concept_id,
            d.udi AS unique_device_id,
            d.serial_num AS production_id,
            1 AS quantity,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            d.source_code AS device_source_value,
            0 AS device_source_concept_id,
            0 AS unit_concept_id,
            NULL::VARCHAR(50) AS unit_source_value,
            0 AS unit_source_concept_id
        FROM dev_flat d
        LEFT JOIN terminology.snomed_to_omop sm
            ON sm.snomed_code = d.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.device_exposure").collect()[0]['CNT']
    return f"Mapped {count} devices to {output_schema}.device_exposure"
$$;
GRANT USAGE ON PROCEDURE core.map_devices(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- DiagnosticReport Mapper — FHIR DiagnosticReport → OMOP measurement (appends)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_diagnostic_reports(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.measurement (
            measurement_id              INTEGER,
            person_id                   INTEGER,
            measurement_concept_id      INTEGER,
            measurement_date            DATE,
            measurement_datetime        TIMESTAMP_NTZ,
            measurement_type_concept_id INTEGER DEFAULT 32817,
            operator_concept_id         INTEGER DEFAULT 0,
            value_as_number             FLOAT,
            value_as_concept_id         INTEGER DEFAULT 0,
            unit_concept_id             INTEGER DEFAULT 0,
            range_low                   FLOAT,
            range_high                  FLOAT,
            provider_id                 INTEGER,
            visit_occurrence_id         INTEGER,
            measurement_source_value    VARCHAR(256),
            measurement_source_concept_id INTEGER DEFAULT 0,
            unit_source_value           VARCHAR(50),
            value_source_value          VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.measurement
        WITH diag_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS diag_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:effectiveDateTime::DATE, r.resource_json:issued::DATE) AS meas_date,
                COALESCE(r.resource_json:effectiveDateTime::TIMESTAMP_NTZ, r.resource_json:issued::TIMESTAMP_NTZ) AS meas_dt,
                dc.value:code::VARCHAR AS source_code,
                COALESCE(dc.value:display::VARCHAR, r.resource_json:code:text::VARCHAR) AS diag_desc
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) dc
            WHERE r.resource_type = 'DiagnosticReport'
        )
        SELECT
            ABS(HASH(d.diag_id)) % 2147483647 AS measurement_id,
            ABS(HASH(d.patient_ref)) % 2147483647 AS person_id,
            COALESCE(lm.omop_concept_id, 0) AS measurement_concept_id,
            d.meas_date AS measurement_date,
            d.meas_dt AS measurement_datetime,
            32817 AS measurement_type_concept_id,
            0 AS operator_concept_id,
            NULL::FLOAT AS value_as_number,
            0 AS value_as_concept_id,
            0 AS unit_concept_id,
            NULL::FLOAT AS range_low,
            NULL::FLOAT AS range_high,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            d.source_code AS measurement_source_value,
            0 AS measurement_source_concept_id,
            NULL::VARCHAR(50) AS unit_source_value,
            d.diag_desc AS value_source_value
        FROM diag_flat d
        LEFT JOIN terminology.loinc_to_omop lm
            ON lm.loinc_code = d.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.measurement").collect()[0]['CNT']
    return f"Mapped DiagnosticReports; total measurement now {count}"
$$;
GRANT USAGE ON PROCEDURE core.map_diagnostic_reports(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- ImagingStudy Mapper — FHIR ImagingStudy → OMOP procedure_occurrence (appends)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_imaging_studies(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.procedure_occurrence (
            procedure_occurrence_id      INTEGER,
            person_id                    INTEGER,
            procedure_concept_id         INTEGER,
            procedure_date               DATE,
            procedure_datetime           TIMESTAMP_NTZ,
            procedure_end_date           DATE,
            procedure_type_concept_id    INTEGER DEFAULT 32817,
            provider_id                  INTEGER,
            visit_occurrence_id          INTEGER,
            procedure_source_value       VARCHAR(256),
            procedure_source_concept_id  INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.procedure_occurrence
        WITH img_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS img_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                r.resource_json:started::DATE AS proc_date,
                r.resource_json:started::TIMESTAMP_NTZ AS proc_dt,
                mc.value:code::VARCHAR AS source_code,
                mc.value:system::VARCHAR AS code_system
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:modality:coding, OUTER => TRUE) mc
            WHERE r.resource_type = 'ImagingStudy'
        )
        SELECT
            ABS(HASH(i.img_id)) % 2147483647 AS procedure_occurrence_id,
            ABS(HASH(i.patient_ref)) % 2147483647 AS person_id,
            COALESCE(sm.omop_concept_id, 0) AS procedure_concept_id,
            i.proc_date AS procedure_date,
            i.proc_dt AS procedure_datetime,
            NULL::DATE AS procedure_end_date,
            32817 AS procedure_type_concept_id,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            i.source_code AS procedure_source_value,
            0 AS procedure_source_concept_id
        FROM img_flat i
        LEFT JOIN terminology.snomed_to_omop sm
            ON sm.snomed_code = i.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.procedure_occurrence").collect()[0]['CNT']
    return f"Mapped ImagingStudies; total procedure_occurrence now {count}"
$$;
GRANT USAGE ON PROCEDURE core.map_imaging_studies(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- CarePlan Mapper — FHIR CarePlan → OMOP observation (appends)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_care_plans(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.observation (
            observation_id                INTEGER,
            person_id                     INTEGER,
            observation_concept_id        INTEGER,
            observation_date              DATE,
            observation_datetime          TIMESTAMP_NTZ,
            observation_type_concept_id   INTEGER DEFAULT 32817,
            value_as_string               VARCHAR(1024),
            value_as_concept_id           INTEGER DEFAULT 0,
            observation_source_value      VARCHAR(256),
            observation_source_concept_id INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.observation
        WITH cp_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS cp_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                r.resource_json:period:start::DATE AS obs_date,
                r.resource_json:period:start::TIMESTAMP_NTZ AS obs_dt,
                cc.value:code::VARCHAR AS source_code,
                COALESCE(cc.value:display::VARCHAR, r.resource_json:title::VARCHAR) AS cp_desc,
                r.resource_json:status::VARCHAR AS cp_status,
                r.resource_json:intent::VARCHAR AS cp_intent
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:category[0]:coding, OUTER => TRUE) cc
            WHERE r.resource_type = 'CarePlan'
        )
        SELECT
            ABS(HASH(c.cp_id)) % 2147483647 AS observation_id,
            ABS(HASH(c.patient_ref)) % 2147483647 AS person_id,
            4149299 AS observation_concept_id,
            c.obs_date AS observation_date,
            c.obs_dt AS observation_datetime,
            32817 AS observation_type_concept_id,
            CONCAT(COALESCE(c.cp_desc, ''), ' [', COALESCE(c.cp_status, ''), '/', COALESCE(c.cp_intent, ''), ']') AS value_as_string,
            0 AS value_as_concept_id,
            c.source_code AS observation_source_value,
            0 AS observation_source_concept_id
        FROM cp_flat c
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.observation").collect()[0]['CNT']
    return f"Mapped CarePlans; total observation now {count}"
$$;
GRANT USAGE ON PROCEDURE core.map_care_plans(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Location Mapper — FHIR Location → OMOP location
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_locations(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.location (
            location_id                 INTEGER,
            address_1                   VARCHAR(256),
            address_2                   VARCHAR(256),
            city                        VARCHAR(100),
            state                       VARCHAR(50),
            zip                         VARCHAR(20),
            county                      VARCHAR(100),
            country_concept_id          INTEGER DEFAULT 0,
            country_source_value        VARCHAR(100),
            latitude                    FLOAT,
            longitude                   FLOAT,
            location_source_value       VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.location
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS location_id,
            r.resource_json:address:line[0]::VARCHAR AS address_1,
            r.resource_json:address:line[1]::VARCHAR AS address_2,
            r.resource_json:address:city::VARCHAR AS city,
            r.resource_json:address:state::VARCHAR AS state,
            r.resource_json:address:postalCode::VARCHAR AS zip,
            r.resource_json:address:district::VARCHAR AS county,
            0 AS country_concept_id,
            r.resource_json:address:country::VARCHAR AS country_source_value,
            r.resource_json:position:latitude::FLOAT AS latitude,
            r.resource_json:position:longitude::FLOAT AS longitude,
            r.resource_json:name::VARCHAR AS location_source_value
        FROM app_state.fhir_resources r
        WHERE r.resource_type = 'Location'
    """).collect()

    p_count = session.sql("""
        SELECT COUNT(*) AS cnt FROM app_state.fhir_resources
        WHERE resource_type = 'Patient' AND resource_json:address IS NOT NULL
    """).collect()[0]['CNT']
    if p_count > 0:
        session.sql(f"""
            INSERT INTO {output_schema}.location
            SELECT
                ABS(HASH(CONCAT(r.resource_json:id::VARCHAR, '_addr'))) % 2147483647 AS location_id,
                r.resource_json:address[0]:line[0]::VARCHAR AS address_1,
                r.resource_json:address[0]:line[1]::VARCHAR AS address_2,
                r.resource_json:address[0]:city::VARCHAR AS city,
                r.resource_json:address[0]:state::VARCHAR AS state,
                r.resource_json:address[0]:postalCode::VARCHAR AS zip,
                r.resource_json:address[0]:district::VARCHAR AS county,
                0 AS country_concept_id,
                r.resource_json:address[0]:country::VARCHAR AS country_source_value,
                NULL::FLOAT AS latitude,
                NULL::FLOAT AS longitude,
                r.resource_json:id::VARCHAR AS location_source_value
            FROM app_state.fhir_resources r
            WHERE r.resource_type = 'Patient'
                AND r.resource_json:address IS NOT NULL
        """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.location").collect()[0]['CNT']
    return f"Mapped {count} locations to {output_schema}.location"
$$;
GRANT USAGE ON PROCEDURE core.map_locations(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Organization Mapper — FHIR Organization → OMOP care_site
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_organizations(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.care_site (
            care_site_id                INTEGER,
            care_site_name              VARCHAR(512),
            place_of_service_concept_id INTEGER DEFAULT 0,
            location_id                 INTEGER,
            care_site_source_value      VARCHAR(256),
            place_of_service_source_value VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.care_site
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS care_site_id,
            r.resource_json:name::VARCHAR AS care_site_name,
            0 AS place_of_service_concept_id,
            NULL AS location_id,
            r.resource_json:id::VARCHAR AS care_site_source_value,
            r.resource_json:type[0]:coding[0]:display::VARCHAR AS place_of_service_source_value
        FROM app_state.fhir_resources r
        WHERE r.resource_type = 'Organization'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.care_site").collect()[0]['CNT']
    return f"Mapped {count} care sites to {output_schema}.care_site"
$$;
GRANT USAGE ON PROCEDURE core.map_organizations(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Practitioner Mapper — FHIR Practitioner → OMOP provider
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_practitioners(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.provider (
            provider_id                 INTEGER,
            provider_name               VARCHAR(512),
            npi                         VARCHAR(20),
            dea                         VARCHAR(20),
            specialty_concept_id        INTEGER DEFAULT 0,
            care_site_id                INTEGER,
            year_of_birth               INTEGER,
            gender_concept_id           INTEGER DEFAULT 0,
            provider_source_value       VARCHAR(256),
            specialty_source_value      VARCHAR(256),
            specialty_source_concept_id INTEGER DEFAULT 0,
            gender_source_value         VARCHAR(50),
            gender_source_concept_id    INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.provider
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS provider_id,
            CONCAT(
                COALESCE(r.resource_json:name[0]:prefix[0]::VARCHAR || ' ', ''),
                COALESCE(r.resource_json:name[0]:given[0]::VARCHAR || ' ', ''),
                COALESCE(r.resource_json:name[0]:family::VARCHAR, '')
            ) AS provider_name,
            r.resource_json:identifier[0]:value::VARCHAR AS npi,
            NULL::VARCHAR(20) AS dea,
            0 AS specialty_concept_id,
            NULL AS care_site_id,
            NULL::INTEGER AS year_of_birth,
            0 AS gender_concept_id,
            r.resource_json:id::VARCHAR AS provider_source_value,
            r.resource_json:qualification[0]:code:coding[0]:display::VARCHAR AS specialty_source_value,
            0 AS specialty_source_concept_id,
            r.resource_json:gender::VARCHAR AS gender_source_value,
            0 AS gender_source_concept_id
        FROM app_state.fhir_resources r
        WHERE r.resource_type = 'Practitioner'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.provider").collect()[0]['CNT']
    return f"Mapped {count} providers to {output_schema}.provider"
$$;
GRANT USAGE ON PROCEDURE core.map_practitioners(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Claim/EOB Mapper — FHIR Claim + ExplanationOfBenefit → OMOP payer_plan_period + cost
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_claims(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.payer_plan_period (
            payer_plan_period_id        INTEGER,
            person_id                   INTEGER,
            payer_plan_period_start_date DATE,
            payer_plan_period_end_date  DATE,
            payer_concept_id            INTEGER DEFAULT 0,
            payer_source_value          VARCHAR(256),
            payer_source_concept_id     INTEGER DEFAULT 0,
            plan_concept_id             INTEGER DEFAULT 0,
            plan_source_value           VARCHAR(256),
            plan_source_concept_id      INTEGER DEFAULT 0,
            sponsor_concept_id          INTEGER DEFAULT 0,
            sponsor_source_value        VARCHAR(256),
            sponsor_source_concept_id   INTEGER DEFAULT 0,
            family_source_value         VARCHAR(256),
            stop_reason_concept_id      INTEGER DEFAULT 0,
            stop_reason_source_value    VARCHAR(256),
            stop_reason_source_concept_id INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.cost (
            cost_id                     INTEGER,
            cost_event_id               INTEGER,
            cost_domain_id              VARCHAR(50),
            cost_type_concept_id        INTEGER DEFAULT 32817,
            currency_concept_id         INTEGER DEFAULT 44818668,
            total_charge                FLOAT,
            total_cost                  FLOAT,
            total_paid                  FLOAT,
            paid_by_payer               FLOAT,
            paid_by_patient             FLOAT,
            paid_patient_copay          FLOAT,
            paid_patient_coinsurance    FLOAT,
            paid_patient_deductible     FLOAT,
            paid_by_primary             FLOAT,
            paid_ingredient_cost        FLOAT,
            paid_dispensing_fee         FLOAT,
            payer_plan_period_id        INTEGER,
            amount_allowed              FLOAT,
            revenue_code_concept_id     INTEGER DEFAULT 0,
            revenue_code_source_value   VARCHAR(50),
            drg_concept_id              INTEGER DEFAULT 0,
            drg_source_value            VARCHAR(50)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.cost
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS cost_id,
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS cost_event_id,
            'Visit' AS cost_domain_id,
            32817 AS cost_type_concept_id,
            44818668 AS currency_concept_id,
            r.resource_json:total:value::FLOAT AS total_charge,
            r.resource_json:total:value::FLOAT AS total_cost,
            r.resource_json:payment:amount:value::FLOAT AS total_paid,
            r.resource_json:payment:amount:value::FLOAT AS paid_by_payer,
            NULL::FLOAT AS paid_by_patient,
            NULL::FLOAT AS paid_patient_copay,
            NULL::FLOAT AS paid_patient_coinsurance,
            NULL::FLOAT AS paid_patient_deductible,
            NULL::FLOAT AS paid_by_primary,
            NULL::FLOAT AS paid_ingredient_cost,
            NULL::FLOAT AS paid_dispensing_fee,
            NULL AS payer_plan_period_id,
            NULL::FLOAT AS amount_allowed,
            0 AS revenue_code_concept_id,
            NULL::VARCHAR(50) AS revenue_code_source_value,
            0 AS drg_concept_id,
            NULL::VARCHAR(50) AS drg_source_value
        FROM app_state.fhir_resources r
        WHERE r.resource_type IN ('Claim', 'ExplanationOfBenefit')
            AND r.resource_json:total:value IS NOT NULL
    """).collect()

    eob_count = session.sql("""
        SELECT COUNT(*) AS cnt FROM app_state.fhir_resources
        WHERE resource_type = 'ExplanationOfBenefit'
            AND resource_json:insurance IS NOT NULL
    """).collect()[0]['CNT']
    if eob_count > 0:
        session.sql(f"""
            INSERT INTO {output_schema}.payer_plan_period
            SELECT
                ABS(HASH(CONCAT(r.resource_json:id::VARCHAR, '_ppp'))) % 2147483647 AS payer_plan_period_id,
                ABS(HASH(SPLIT_PART(r.resource_json:patient:reference::VARCHAR, '/', -1))) % 2147483647 AS person_id,
                r.resource_json:billablePeriod:start::DATE AS payer_plan_period_start_date,
                COALESCE(r.resource_json:billablePeriod:end::DATE, r.resource_json:billablePeriod:start::DATE) AS payer_plan_period_end_date,
                0 AS payer_concept_id,
                r.resource_json:insurer:display::VARCHAR AS payer_source_value,
                0 AS payer_source_concept_id,
                0 AS plan_concept_id,
                r.resource_json:insurance[0]:coverage:display::VARCHAR AS plan_source_value,
                0 AS plan_source_concept_id,
                0 AS sponsor_concept_id,
                NULL::VARCHAR(256) AS sponsor_source_value,
                0 AS sponsor_source_concept_id,
                NULL::VARCHAR(256) AS family_source_value,
                0 AS stop_reason_concept_id,
                NULL::VARCHAR(256) AS stop_reason_source_value,
                0 AS stop_reason_source_concept_id
            FROM app_state.fhir_resources r
            WHERE r.resource_type = 'ExplanationOfBenefit'
                AND r.resource_json:insurance IS NOT NULL
        """).collect()

    cost_count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.cost").collect()[0]['CNT']
    ppp_count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.payer_plan_period").collect()[0]['CNT']
    return f"Mapped {cost_count} costs, {ppp_count} payer_plan_periods"
$$;
GRANT USAGE ON PROCEDURE core.map_claims(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- CareTeam Mapper — FHIR CareTeam → OMOP fact_relationship
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_care_teams(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.fact_relationship (
            domain_concept_id_1     INTEGER,
            fact_id_1               INTEGER,
            domain_concept_id_2     INTEGER,
            fact_id_2               INTEGER,
            relationship_concept_id INTEGER
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.fact_relationship
        WITH ct_members AS (
            SELECT
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                SPLIT_PART(m.value:member:reference::VARCHAR, '/', -1) AS provider_ref,
                m.value:role[0]:coding[0]:code::VARCHAR AS role_code
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:participant) m
            WHERE r.resource_type = 'CareTeam'
                AND m.value:member:reference IS NOT NULL
        )
        SELECT
            56 AS domain_concept_id_1,
            ABS(HASH(c.patient_ref)) % 2147483647 AS fact_id_1,
            58 AS domain_concept_id_2,
            ABS(HASH(c.provider_ref)) % 2147483647 AS fact_id_2,
            44818821 AS relationship_concept_id
        FROM ct_members c
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.fact_relationship").collect()[0]['CNT']
    return f"Mapped {count} care team relationships to {output_schema}.fact_relationship"
$$;
GRANT USAGE ON PROCEDURE core.map_care_teams(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Observation Period — REQUIRED OMOP table, derived from all clinical events
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.build_observation_periods(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.observation_period (
            observation_period_id           INTEGER,
            person_id                       INTEGER,
            observation_period_start_date   DATE,
            observation_period_end_date     DATE,
            period_type_concept_id          INTEGER DEFAULT 32817
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.observation_period
        WITH all_dates AS (
            SELECT person_id, condition_start_date AS event_date FROM {output_schema}.condition_occurrence WHERE condition_start_date IS NOT NULL
            UNION ALL
            SELECT person_id, measurement_date AS event_date FROM {output_schema}.measurement WHERE measurement_date IS NOT NULL
            UNION ALL
            SELECT person_id, visit_start_date AS event_date FROM {output_schema}.visit_occurrence WHERE visit_start_date IS NOT NULL
            UNION ALL
            SELECT person_id, drug_exposure_start_date AS event_date FROM {output_schema}.drug_exposure WHERE drug_exposure_start_date IS NOT NULL
            UNION ALL
            SELECT person_id, procedure_date AS event_date FROM {output_schema}.procedure_occurrence WHERE procedure_date IS NOT NULL
            UNION ALL
            SELECT person_id, observation_date AS event_date FROM {output_schema}.observation WHERE observation_date IS NOT NULL
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY person_id) AS observation_period_id,
            person_id,
            MIN(event_date) AS observation_period_start_date,
            MAX(event_date) AS observation_period_end_date,
            32817 AS period_type_concept_id
        FROM all_dates
        GROUP BY person_id
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.observation_period").collect()[0]['CNT']
    return f"Built {count} observation periods in {output_schema}.observation_period"
$$;
GRANT USAGE ON PROCEDURE core.build_observation_periods(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- CDM Source — REQUIRED OMOP metadata table
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.build_cdm_source(output_schema VARCHAR DEFAULT 'omop_staging')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE OR REPLACE TABLE {output_schema}.cdm_source (
            cdm_source_name                 VARCHAR(256),
            cdm_source_abbreviation         VARCHAR(50),
            cdm_holder                      VARCHAR(256),
            source_description              VARCHAR(4096),
            source_documentation_reference  VARCHAR(1024),
            cdm_etl_reference               VARCHAR(1024),
            source_release_date             DATE,
            cdm_release_date                DATE,
            cdm_version                     VARCHAR(20),
            cdm_version_concept_id          INTEGER DEFAULT 756265,
            vocabulary_version              VARCHAR(50)
        )
    """).collect()

    bundle_count = session.sql("SELECT COUNT(DISTINCT bundle_id) AS cnt FROM app_state.fhir_resources").collect()[0]['CNT']
    resource_count = session.sql("SELECT COUNT(*) AS cnt FROM app_state.fhir_resources").collect()[0]['CNT']

    session.sql(f"""
        INSERT INTO {output_schema}.cdm_source VALUES (
            'Tuva FHIR-to-OMOP Native App',
            'TUVA_FHIR_OMOP',
            'Snowflake Native App',
            'Automated FHIR R4 to OMOP CDM v5.4 transformation. {bundle_count} bundles, {resource_count} resources processed.',
            'https://github.com/JacinthLaval/tuva-fhir-to-omop-app',
            'Tuva FHIR-to-OMOP ETL v1.2',
            CURRENT_DATE(),
            CURRENT_DATE(),
            'v5.4',
            756265,
            'OHDSI Athena + Tuva Health seed patterns'
        )
    """).collect()

    return "CDM source metadata written"
$$;
GRANT USAGE ON PROCEDURE core.build_cdm_source(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- FHIR Quality Validator — read-only diagnostic returning JSON summary
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.validate_fhir_quality()
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
import json

def run(session) -> str:
    summary = {}

    total_bundles = session.sql(
        "SELECT COUNT(DISTINCT bundle_id) AS cnt FROM app_state.fhir_resources"
    ).collect()[0]['CNT']
    summary['total_bundles'] = total_bundles

    type_rows = session.sql(
        "SELECT resource_type, COUNT(*) AS cnt FROM app_state.fhir_resources GROUP BY resource_type ORDER BY cnt DESC"
    ).collect()
    summary['resources_by_type'] = {row['RESOURCE_TYPE']: row['CNT'] for row in type_rows}

    issues = []

    checks = [
        ("Patient missing birthDate", "resource_type = 'Patient' AND resource_json:birthDate IS NULL"),
        ("Patient missing id", "resource_type = 'Patient' AND resource_json:id IS NULL"),
        ("Condition missing code", "resource_type = 'Condition' AND resource_json:code IS NULL"),
        ("Observation missing value", "resource_type = 'Observation' AND resource_json:valueQuantity IS NULL AND resource_json:valueCodeableConcept IS NULL AND resource_json:valueString IS NULL AND resource_json:component IS NULL"),
        ("Encounter missing period.start", "resource_type = 'Encounter' AND resource_json:period:start IS NULL"),
        ("MedicationRequest missing medication", "resource_type = 'MedicationRequest' AND resource_json:medicationCodeableConcept IS NULL AND resource_json:medicationReference IS NULL"),
        ("MedicationAdministration missing medication", "resource_type = 'MedicationAdministration' AND resource_json:medicationCodeableConcept IS NULL AND resource_json:medicationReference IS NULL"),
        ("Procedure missing code", "resource_type = 'Procedure' AND resource_json:code IS NULL"),
        ("Immunization missing vaccineCode", "resource_type = 'Immunization' AND resource_json:vaccineCode IS NULL"),
        ("AllergyIntolerance missing code", "resource_type = 'AllergyIntolerance' AND resource_json:code IS NULL"),
        ("Device missing type", "resource_type = 'Device' AND resource_json:type IS NULL"),
        ("DiagnosticReport missing code", "resource_type = 'DiagnosticReport' AND resource_json:code IS NULL"),
        ("ImagingStudy missing modality", "resource_type = 'ImagingStudy' AND resource_json:modality IS NULL"),
        ("Claim missing total", "resource_type = 'Claim' AND resource_json:total:value IS NULL"),
        ("CarePlan missing category", "resource_type = 'CarePlan' AND resource_json:category IS NULL"),
    ]

    for rule, where in checks:
        cnt = session.sql(f"SELECT COUNT(*) AS cnt FROM app_state.fhir_resources WHERE {where}").collect()[0]['CNT']
        if cnt > 0:
            issues.append({'rule': rule, 'count': cnt})

    summary['quality_issues'] = issues
    summary['total_issues'] = sum(i['count'] for i in issues)

    return json.dumps(summary, indent=2)
$$;
GRANT USAGE ON PROCEDURE core.validate_fhir_quality()
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Orchestrator — runs all mappers in sequence
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.run_full_transformation(
    source_table   VARCHAR,
    json_column    VARCHAR DEFAULT 'BUNDLE_DATA',
    output_schema  VARCHAR DEFAULT 'omop_staging',
    bundle_id_column VARCHAR DEFAULT 'BUNDLE_ID'
)
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
import json

def run(session, source_table: str, json_column: str, output_schema: str, bundle_id_column: str) -> str:
    run_id = session.sql("SELECT UUID_STRING()").collect()[0][0]
    session.sql(f"""
        INSERT INTO app_state.run_history (run_id, status)
        VALUES ('{run_id}', 'RUNNING')
    """).collect()

    results = {}
    errors = []
    try:
        bundle_id_col = bundle_id_column
        if not bundle_id_col or bundle_id_col == 'BUNDLE_ID':
            try:
                cols = [r['COLUMN_NAME'].upper() for r in session.sql(f"SHOW COLUMNS IN TABLE {source_table}").collect()]
                if 'BUNDLE_ID' in cols:
                    bundle_id_col = 'BUNDLE_ID'
                elif 'MESSAGE_ID' in cols:
                    bundle_id_col = 'MESSAGE_ID'
                else:
                    id_cols = [c for c in cols if 'ID' in c and c != json_column.upper()]
                    bundle_id_col = id_cols[0] if id_cols else json_column
            except:
                pass

        r = session.call('core.parse_fhir_bundles', source_table, json_column, bundle_id_col)
        results['parse'] = r

        for mapper, key in [
            ('core.map_persons', 'persons'),
            ('core.map_conditions', 'conditions'),
            ('core.map_measurements', 'measurements'),
            ('core.map_visits', 'visits'),
            ('core.map_drug_exposures', 'drugs'),
            ('core.map_procedures', 'procedures'),
            ('core.map_observations_qual', 'observations_qual'),
            ('core.map_death', 'death'),
            ('core.map_immunizations', 'immunizations'),
            ('core.map_med_administrations', 'med_administrations'),
            ('core.map_allergies', 'allergies'),
            ('core.map_devices', 'devices'),
            ('core.map_diagnostic_reports', 'diagnostic_reports'),
            ('core.map_imaging_studies', 'imaging_studies'),
            ('core.map_care_plans', 'care_plans'),
            ('core.map_locations', 'locations'),
            ('core.map_organizations', 'organizations'),
            ('core.map_practitioners', 'practitioners'),
            ('core.map_claims', 'claims'),
            ('core.map_care_teams', 'care_teams'),
            ('core.build_observation_periods', 'obs_periods'),
            ('core.build_cdm_source', 'cdm_source'),
        ]:
            try:
                r = session.call(mapper, output_schema)
                results[key] = r
            except Exception as e:
                errors.append(f"{mapper}: {str(e)}")

        counts = {}
        for tbl, col in [
            ('person', 'persons_mapped'),
            ('condition_occurrence', 'conditions_mapped'),
            ('measurement', 'measurements_mapped'),
            ('visit_occurrence', 'visits_mapped'),
            ('drug_exposure', 'drugs_mapped'),
            ('procedure_occurrence', 'procedures_mapped'),
            ('observation', 'observations_mapped'),
            ('death', 'death_mapped'),
            ('device_exposure', 'devices_mapped'),
            ('location', 'locations_mapped'),
            ('care_site', 'care_sites_mapped'),
            ('provider', 'providers_mapped'),
            ('cost', 'costs_mapped'),
            ('fact_relationship', 'fact_rels_mapped'),
            ('observation_period', 'obs_periods_built'),
            ('payer_plan_period', 'payer_plans_mapped'),
        ]:
            try:
                cnt = session.sql(f"SELECT COUNT(*) AS c FROM {output_schema}.{tbl}").collect()[0]['C']
                counts[col] = cnt
            except:
                counts[col] = 0

        bundles = session.sql(f"SELECT COUNT(*) AS c FROM {source_table}").collect()[0]['C']

        session.sql(f"""
            UPDATE app_state.run_history SET
                status = '{'COMPLETED_WITH_ERRORS' if errors else 'COMPLETED'}',
                completed_at = CURRENT_TIMESTAMP(),
                fhir_bundles = {bundles},
                persons_mapped = {counts.get('persons_mapped', 0)},
                conditions_mapped = {counts.get('conditions_mapped', 0)},
                measurements_mapped = {counts.get('measurements_mapped', 0)},
                visits_mapped = {counts.get('visits_mapped', 0)},
                drugs_mapped = {counts.get('drugs_mapped', 0)},
                procedures_mapped = {counts.get('procedures_mapped', 0)},
                observations_mapped = {counts.get('observations_mapped', 0)},
                death_mapped = {counts.get('death_mapped', 0)},
                devices_mapped = {counts.get('devices_mapped', 0)},
                locations_mapped = {counts.get('locations_mapped', 0)},
                care_sites_mapped = {counts.get('care_sites_mapped', 0)},
                providers_mapped = {counts.get('providers_mapped', 0)},
                costs_mapped = {counts.get('costs_mapped', 0)},
                fact_rels_mapped = {counts.get('fact_rels_mapped', 0)},
                obs_periods_built = {counts.get('obs_periods_built', 0)},
                payer_plans_mapped = {counts.get('payer_plans_mapped', 0)},
                errors = {len(errors)},
                error_detail = '{json.dumps(errors).replace("'", "''")}'
            WHERE run_id = '{run_id}'
        """).collect()

    except Exception as e:
        session.sql(f"""
            UPDATE app_state.run_history SET
                status = 'FAILED',
                completed_at = CURRENT_TIMESTAMP(),
                errors = 1,
                error_detail = '{str(e).replace("'", "''")}'
            WHERE run_id = '{run_id}'
        """).collect()
        return f"FAILED: {str(e)}"

    summary_parts = [f"Transformation complete!"]
    summary_parts.append(f"Source: {source_table} ({bundles:,} records)")
    summary_parts.append(f"Output: {output_schema}")
    summary_parts.append("")
    table_labels = {
        'persons_mapped': 'Persons', 'conditions_mapped': 'Conditions',
        'measurements_mapped': 'Measurements', 'visits_mapped': 'Visits',
        'drugs_mapped': 'Drug Exposures', 'procedures_mapped': 'Procedures',
        'observations_mapped': 'Observations', 'death_mapped': 'Death Records',
        'devices_mapped': 'Devices', 'locations_mapped': 'Locations',
        'care_sites_mapped': 'Care Sites', 'providers_mapped': 'Providers',
        'costs_mapped': 'Costs', 'fact_rels_mapped': 'Fact Relationships',
        'obs_periods_built': 'Observation Periods', 'payer_plans_mapped': 'Payer Plans',
    }
    for col, label in table_labels.items():
        cnt = counts.get(col, 0)
        if cnt > 0:
            summary_parts.append(f"  {label}: {cnt:,}")
    if errors:
        summary_parts.append("")
        summary_parts.append(f"{len(errors)} mapper(s) had errors:")
        for e in errors:
            short = e.split(':')[0] + ': ' + e.split(':')[-1].strip()[:80] if ':' in e else e[:100]
            summary_parts.append(f"  - {short}")
    return '\n'.join(summary_parts)
$$;
GRANT USAGE ON PROCEDURE core.run_full_transformation(VARCHAR, VARCHAR, VARCHAR, VARCHAR)
    TO APPLICATION ROLE app_admin;
