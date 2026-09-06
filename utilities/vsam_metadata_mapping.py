# tests/vsam/metadata_zips/ holds the actual zip files this maps to.
# Mirrors utilities/ims_dbd_mapping.py's TEST_DBD_MAP exactly - flat dict,
# test folder name -> filename, nothing else.

TEST_METADATA_MAP = {
    "alltypes_matrix": "alltypes_matrix.zip",

    "array_no_dep": "array_no_dep.zip",
    "array_with_dep": "array_with_dep.zip",
    "array_comp_counter": "array_comp_counter.zip",
    "array_unequal_redefines": "array_unequal_redefines.zip",
    "array_deep_nest": "array_deep_nest.zip",
    "array_long_field": "array_long_field.zip",
    "array_in_array_depon": "array_in_array_depon.zip",

    "primitive_array_fixed_string": "arrays.zip",
    "primitive_array_fixed_number": "arrays.zip",
    "primitive_array_dynamic_string": "arrays.zip",
    "primitive_array_dynamic_number": "arrays.zip",
    "primitive_array_nested_fixed": "arrays.zip",

    "policy_ksds": "policy.zip",
    "order_ksds": "order.zip",
    "employee_ksds": "employee.zip",

    "rrds_full_load": "struct1_complex.zip",
    "esds_full_load": "struct2.zip",
}
