use concordium_base::smart_contracts::WasmModule;
use concordium_contracts_common::{
    from_bytes, schema::VersionedModuleSchema, to_bytes, WasmVersion,
};
use concordium_smart_contract_engine::utils;
use pyo3::{exceptions::*, prelude::*};

/// A Python module implemented in Rust.
#[pymodule]
fn schema_parsing(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(extract_schema_ffi, m)?)?;
    m.add_function(wrap_pyfunction!(extract_schema_pair_ffi, m)?)?;
    m.add_function(wrap_pyfunction!(parse_event_ffi, m)?)?;
    m.add_function(wrap_pyfunction!(parse_return_value_ffi, m)?)?;
    m.add_function(wrap_pyfunction!(parse_parameter_ffi, m)?)?;
    Ok(())
}

#[pyfunction]
fn extract_schema_ffi(versioned_module_source: Vec<u8>) -> PyResult<Vec<u8>> {
    let module = match WasmModule::from_slice(&versioned_module_source) {
        Ok(m) => m,
        Err(e) => {
            return Err(PyValueError::new_err(format!(
                "Malformed module provided: {e}."
            )))
        }
    };

    let schema = match module.version {
        WasmVersion::V0 => utils::get_embedded_schema_v0(module.source.as_ref()),
        WasmVersion::V1 => utils::get_embedded_schema_v1(module.source.as_ref()),
    };
    let schema = match schema {
        Ok(s) => s,
        Err(e) => {
            return Err(PyValueError::new_err(format!(
                "Unable to get schema from the module: {e}"
            )))
        }
    };
    Ok(to_bytes(&schema))
}

#[pyfunction]
fn extract_schema_pair_ffi(module_version: u8, module_source: Vec<u8>) -> PyResult<Vec<u8>> {
    let schema = match module_version {
        0 => utils::get_embedded_schema_v0(module_source.as_ref()),
        1 => utils::get_embedded_schema_v1(module_source.as_ref()),
        v => {
            return Err(PyValueError::new_err(format!(
                "Unrecognized module version: {v}"
            )))
        }
    };
    let schema = match schema {
        Ok(s) => s,
        Err(e) => {
            return Err(PyValueError::new_err(format!(
                "Unable to get schema from the module: {e}"
            )))
        }
    };
    Ok(to_bytes(&schema))
}

#[pyfunction]
fn parse_event_ffi(
    versioned_module_schema: Vec<u8>,
    contract_name: &str,
    event_data: Vec<u8>,
) -> PyResult<String> {
    let schema: VersionedModuleSchema = match from_bytes(&versioned_module_schema) {
        Ok(s) => s,
        Err(e) => {
            return Err(PyValueError::new_err(format!(
                "Unable to parse schema: {e}"
            )))
        }
    };
    match schema.get_event_schema(contract_name) {
        Ok(s) => match s.to_json_string_pretty(&event_data) {
            Ok(v) => Ok(v),
            Err(e) => Err(PyValueError::new_err(format!("Unable to parse event: {e}"))),
        },
        Err(e) => Err(PyValueError::new_err(format!("No event schema: {e}"))),
    }
}

#[pyfunction]
fn parse_return_value_ffi(
    versioned_module_schema: Vec<u8>,
    contract_name: &str,
    function_name: &str,
    return_value_data: Vec<u8>,
) -> PyResult<String> {
    let schema: VersionedModuleSchema = match from_bytes(&versioned_module_schema) {
        Ok(s) => s,
        Err(e) => {
            return Err(PyValueError::new_err(format!(
                "Unable to parse schema: {e}"
            )))
        }
    };
    match schema.get_receive_return_value_schema(contract_name, function_name) {
        Ok(s) => match s.to_json_string_pretty(&return_value_data) {
            Ok(v) => Ok(v),
            Err(e) => Err(PyValueError::new_err(format!("Unable to parse event: {e}"))),
        },
        Err(e) => Err(PyValueError::new_err(format!("No event schema: {e}"))),
    }
}

#[pyfunction]
fn parse_parameter_ffi(
    versioned_module_schema: Vec<u8>,
    contract_name: &str,
    function_name: &str,
    parameter_data: Vec<u8>,
) -> PyResult<String> {
    let schema: VersionedModuleSchema = match from_bytes(&versioned_module_schema) {
        Ok(s) => s,
        Err(e) => {
            return Err(PyValueError::new_err(format!(
                "Unable to parse schema: {e}"
            )))
        }
    };
    match schema.get_receive_param_schema(contract_name, function_name) {
        Ok(s) => match s.to_json_string_pretty(&parameter_data) {
            Ok(v) => Ok(v),
            Err(e) => Err(PyValueError::new_err(format!("Unable to parse event: {e}"))),
        },
        Err(e) => Err(PyValueError::new_err(format!("No event schema: {e}"))),
    }
}
