from datapool_client.core.abstractions import (
    BinaryData,
    DataPoolBaseDatabase,
    LabResult,
    MetaActionType,
    MetaData,
    MetaDataHistory,
    MetaFlag,
    MetaLogType,
    MetaPicture,
    Variable,
    Person,
    Picture,
    Project,
    Quality,
    Signal,
    Site,
    Source,
    SourceType,
    SpecialValueDefinition,
)


class DataPool(DataPoolBaseDatabase):
    def __init__(
        self,
        host=None,
        port=None,
        database=None,
        user=None,
        password=None,
        instance=None,
        to_replace={},
        verbose=True,
    ):
        conn_details = dict(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            instance=instance,
            to_replace=to_replace,
            verbose=verbose,
        )
        super().__init__(**conn_details)
        self.variable = Variable(**conn_details, check=False)
        self.signal = Signal(**conn_details, check=False)
        self.site = Site(**conn_details, check=False)
        self.source = Source(**conn_details, check=False)
        self.source_type = SourceType(**conn_details, check=False)
        self.special_value_definition = SpecialValueDefinition(
            **conn_details, check=False
        )
        self.quality = Quality(**conn_details, check=False)
        self.person = Person(**conn_details, check=False)
        self.project = Project(**conn_details, check=False)
        self.meta_data_history = MetaDataHistory(**conn_details, check=False)
        self.meta_flag = MetaFlag(**conn_details, check=False)
        self.meta_data = MetaData(**conn_details, check=False)
        self.meta_log_type = MetaLogType(**conn_details, check=False)
        self.meta_action_type = MetaActionType(**conn_details, check=False)
        self.binary_data = BinaryData(**conn_details, check=False)
        self.lab_result = LabResult(**conn_details, check=False)
        self.picture = Picture(**conn_details, check=False)
        self.meta_picture = MetaPicture(**conn_details, check=False)
