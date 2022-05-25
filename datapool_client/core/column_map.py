COLUMN_MAP = {
    "parameter_from_source_type": ["value"],
    "site_all_img_count": [
        "name",
        "coord_x",
        "coord_y",
        "coord_z",
        "street",
        "postcode",
        "n_images",
        "description",
    ],
    "site_from_source": ["site"],
    "meta_data_get": ["source", "site", "description", "additional_meta_info"],
    "meta_data_history_get": [
        "source",
        "site",
        "log_type",
        "action_type",
        "meta_flag",
        "person",
        "start",
        "end",
        "comment",
        "additional_meta_info",
    ],
    "source_from_parameter": ["source"],
    "source_get_range": ["min_ts", "max_ts"],
    "special_value_definition_from_source_type": ["value", "description"],
    "source_type_from_source": ["value"],
    "signal_get_if": ["timestamp", "id"],
    "signal_get_with_quality": [
        "timestamp",
        "value",
        "unit",
        "parameter",
        "source",
        "serial",
        "source_type",
        "site",
        "quality_method",
        "quality_flag",
    ],
    "signal_get_without_quality": [
        "timestamp",
        "value",
        "unit",
        "parameter",
        "source",
        "serial",
        "source_type",
        "site",
    ],
    "signal_get_without_quality_and_minimal": [
        "timestamp",
        "value",
        "unit",
        "parameter",
    ],
    "signal_get_minimal": [
        "timestamp",
        "value",
        "unit",
        "parameter",
        "quality_method",
        "quality_flag",
    ],
    "signal_newest": [
        "timestamp",
        "value",
        "unit",
        "parameter",
        "source",
        "serial",
        "source_type",
    ],
    "signal_last": ["source", "timestamp"],
    "signal_from_site_with_rangecheck": [
        "timestamp",
        "value",
        "unit",
        "parameter",
        "source_type",
        "source_instance",
    ],
}
