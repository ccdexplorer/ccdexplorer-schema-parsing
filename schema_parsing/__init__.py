from .schema_parsing import extract_schema_ffi, parse_event_ffi

import json

class Schema:
    def __init__(self, source):
        self.schema = extract_schema_ffi(source)

    def event_to_json(self, contractName, eventData):
        response = parse_event_ffi(self.schema, contractName, eventData)
        return json.loads(response)

    def return_value_to_json(self, contractName, functionName, returnValueData):
        response = parse_return_value_ffi(self.schema, contractName, functionName, returnValueData)
        return json.loads(response)
