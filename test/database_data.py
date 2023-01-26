DB_DATA = {
    "signal": [
        {
            "source_id": 1,
            "site_id": 1,
            "variable_id": 1,
            "value": 11,
            "timestamp": "2000-01-01 10:00:00",
        },
        {
            "source_id": 1,
            "site_id": 1,
            "variable_id": 2,
            "value": 21,
            "timestamp": "2000-01-01 11:00:00",
        },
        {
            "source_id": 1,
            "site_id": 1,
            "variable_id": 1,
            "value": 12,
            "timestamp": "2000-01-02 12:00:00",
        },
        {
            "source_id": 1,
            "site_id": 1,
            "variable_id": 1,
            "value": 13,
            "timestamp": "2000-01-03 10:00:00",
        },
        {
            "source_id": 2,
            "site_id": 1,
            "variable_id": 1,
            "value": 21,
            "timestamp": "2000-01-01 11:00:00",
        },
        {
            "source_id": 2,
            "site_id": 1,
            "variable_id": 2,
            "value": 22,
            "timestamp": "2000-01-01 12:00:00",
        },
        {
            "source_id": 2,
            "site_id": 1,
            "variable_id": 3,
            "value": 23,
            "timestamp": "2000-01-01 13:00:00",
        },
        {
            "source_id": 3,
            "site_id": 1,
            "variable_id": 3,
            "value": 33,
            "timestamp": "2000-01-01 14:00:00",
        },
        {
            "source_id": 4,
            "site_id": 2,
            "variable_id": 1,
            "value": 41,
            "timestamp": "2000-01-01 15:00:00",
        },
        {
            "source_id": 5,
            "site_id": 2,
            "variable_id": 2,
            "value": 52,
            "timestamp": "2000-01-01 16:00:00",
        },
        {
            "source_id": 6,
            "site_id": 2,
            "variable_id": 3,
            "value": 63,
            "timestamp": "2000-01-01 17:00:00",
        },
    ],
    "site": [
        {"site_id": "1", "name": "site_1", "description": "desc to s1"},
        {"site_id": "2", "name": "site_2", "description": "desc to s2"},
    ],
    "site_field": [
        {"site_field_id": "1", "name": "city"},
        {"site_field_id": "2", "name": "country"},
    ],
    "site_field_value": [
        {
            "site_field_value_id": "1",
            "site_id": "1",
            "site_field_id": "1",
            "value": "Zurich",
        },
        {
            "site_field_value_id": "2",
            "site_id": "1",
            "site_field_id": "2",
            "value": "Switzerland",
        },
        {
            "site_field_value_id": "3",
            "site_id": "2",
            "site_field_id": "1",
            "value": "Berlin",
        },
        {
            "site_field_value_id": "4",
            "site_id": "2",
            "site_field_id": "2",
            "value": "Germany",
        },
    ],
    "source": [
        {
            "source_id": "1",
            "source_type_id": "1",
            "project_id": "1",
            "name": "source_1_1",
            "description": "desc to s1_1",
        },
        {
            "source_id": "2",
            "source_type_id": "1",
            "project_id": "1",
            "name": "source_1_2",
            "description": "desc to s1_2",
        },
        {
            "source_id": "3",
            "source_type_id": "2",
            "project_id": "1",
            "name": "source_2_1",
            "description": "desc to s2_1",
        },
        {
            "source_id": "4",
            "source_type_id": "2",
            "project_id": "1",
            "name": "source_2_2",
            "description": "desc to s2_2",
        },
        {
            "source_id": "5",
            "source_type_id": "3",
            "project_id": "1",
            "name": "source_3_1",
            "description": "desc to s3_1",
        },
        {
            "source_id": "6",
            "source_type_id": "3",
            "project_id": "2",
            "name": "source_3_2",
            "description": "desc to s3_2",
        },
    ],
    "variable": [
        {"variable_id": "1", "name": "variable_1", "description": "desc to p1"},
        {"variable_id": "2", "name": "variable_2", "description": "desc to p2"},
        {"variable_id": "3", "name": "variable_3", "description": "desc to p3"},
    ],
    "source_type": [
        {"name": "source_type_1", "description": "desc to st1"},
        {"name": "source_type_2", "description": "desc to st2"},
        {"name": "source_type_3", "description": "desc to st3"},
    ],
    "binary_data": [],
    "lab_result": [],
    "lab_result_person_sample_association": [],
    "lab_result_person_lab_association": [],
    "meta_action_type": [
        {
            "meta_action_type_id": "1",
            "name": "source_maintenance",
            "description": "desc",
        },
        {
            "meta_action_type_id": "2",
            "name": "source_installation",
            "description": "desc",
        },
        {
            "meta_action_type_id": "3",
            "name": "operational_malfunction",
            "description": "desc",
        },
        {
            "meta_action_type_id": "4",
            "name": "miscellaneous",
            "description": "desc",
        },
    ],
    "meta_log_type": [
        {"meta_log_type_id": "1", "name": "source_maintenance", "description": "desc"},
        {"meta_log_type_id": "2", "name": "source_installation", "description": "desc"},
        {
            "meta_log_type_id": "3",
            "name": "operational_malfunction",
            "description": "desc",
        },
        {"meta_log_type_id": "4", "name": "miscellaneous", "description": "desc"},
    ],
    "meta_flag": [{"meta_flag_id": "1", "name": "log_flag_1", "description": "desc"}],
    "meta_picture": [],
    "meta_data": [
        {
            "meta_data_id": "1",
            "source_id": "1",
            "site_id": "1",
            "description": "desc",
            "additional_meta_info": '[{"entry": 4}]',
        }
    ],
    "meta_data_history": [
        {
            "meta_data_history_id": 1,
            "meta_data_id": 1,
            "meta_log_type_id": 1,
            "meta_action_type_id": 1,
            "meta_flag_id": 1,
            "person_id": 1,
            "timestamp_start": "2000-01-01 10:00:00",
            "timestamp_end": "2000-01-01 20:00:00",
            "comment": "comment",
            "additional_meta_info": '[{"entry": 4}, {"entry_2": 88}]',
        },
        {
            "meta_data_history_id": 2,
            "meta_data_id": 1,
            "meta_log_type_id": 2,
            "meta_action_type_id": 2,
            "meta_flag_id": 1,
            "person_id": 1,
            "timestamp_start": "2000-01-02 14:00:00",
            "timestamp_end": "2000-01-02 24:00:00",
            "comment": "comment",
            "additional_meta_info": '[{"entry": 4}, {"entry_2": 88}]',
        },
        {
            "meta_data_history_id": 3,
            "meta_data_id": 1,
            "meta_log_type_id": 3,
            "meta_action_type_id": 3,
            "meta_flag_id": 1,
            "person_id": 1,
            "timestamp_start": "2000-01-02 10:00:00",
            "timestamp_end": "2000-01-02 20:00:00",
            "comment": "comment",
            "additional_meta_info": '[{"entry": 4}, {"entry_2": 88}]',
        },
        {
            "meta_data_history_id": 4,
            "meta_data_id": 1,
            "meta_log_type_id": 4,
            "meta_action_type_id": 4,
            "meta_flag_id": 1,
            "person_id": 1,
            "timestamp_start": "2000-01-01 14:00:00",
            "timestamp_end": "2000-01-01 24:00:00",
            "comment": "comment",
            "additional_meta_info": '[{"entry": 4}, {"entry_2": 88}]',
        },
    ],
    "person": [
        {
            "person_id": "1",
            "name": "person_1",
            "abbreviation": "P1",
            "email": "P1@email",
        },
        {
            "person_id": "2",
            "name": "person_2",
            "abbreviation": "P2",
            "email": "P2@email",
        },
    ],
    "project": [
        {"project_id": "1", "name": "project_1"},
        {"project_id": "2", "name": "project_2"},
    ],
    "quality": [
        {"quality_id": "1", "flag": "green", "method": "filter"},
        {"quality_id": "2", "flag": "red", "method": "filter"},
    ],
    "signal_quality": [
        {
            "signal_quality_id": "1",
            "quality_id": "1",
            "timestamp": "2021-07-01 00:00:00",
            "author": "person_1",
        },
        {
            "signal_quality_id": "2",
            "quality_id": "2",
            "timestamp": "2021-07-01 00:00:00",
            "author": "person_1",
        },
    ],
    "signals_signal_quality_association": [
        {"signal_quality_id": "1", "signal_id": "1"},
        {"signal_quality_id": "2", "signal_id": "1"},
        {"signal_quality_id": "1", "signal_id": "2"},
        {"signal_quality_id": "2", "signal_id": "2"},
        {"signal_quality_id": "1", "signal_id": "3"},
        {"signal_quality_id": "2", "signal_id": "3"},
    ],
    "picture": [],
    "special_value_definition": [
        {
            "source_type_id": 1,
            "description": "desc",
            "categorial_value": "Not a Number",
            "numerical_value": "-99999999",
        }
    ],
}
