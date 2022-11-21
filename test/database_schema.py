from sqlalchemy import (
    JSON,
    REAL,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
)
from sqlalchemy.ext.declarative import declarative_base


def db_schema(db_engine):
    base = declarative_base()

    class Signal(base):
        __tablename__ = "signal"
        signal_id = Column(Integer, primary_key=True)
        timestamp = Column(DateTime)
        value = Column(REAL)
        variable_id = Column(Integer, ForeignKey("variable.variable_id"))
        source_id = Column(Integer, ForeignKey("source.source_id"))
        site_id = Column(Integer, ForeignKey("site.site_id"))

    class Site(base):
        __tablename__ = "site"
        site_id = Column(Integer, primary_key=True)
        name = Column(String)
        description = Column(String)
        street = Column(String)
        postcode = Column(String)
        city = Column(String)
        coord_x = Column(REAL)
        coord_y = Column(REAL)
        coord_z = Column(REAL)

    class Variable(base):
        __tablename__ = "variable"
        variable_id = Column(Integer, primary_key=True)
        name = Column(String)
        description = Column(String)
        unit = Column(String)

    class Source(base):
        __tablename__ = "source"
        source_id = Column(Integer, primary_key=True)
        project_id = Column(ForeignKey("project.project_id"))
        source_type_id = Column(Integer, ForeignKey("source_type.source_type_id"))
        name = Column(String)
        description = Column(String)
        serial = Column(String)

    class SourceType(base):
        __tablename__ = "source_type"
        source_type_id = Column(Integer, primary_key=True)
        name = Column(String)
        description = Column(String)

    class BinaryData(base):
        __tablename__ = "binary_data"
        binary_data_id = Column(Integer, primary_key=True)
        data = Column(LargeBinary)
        variable_id = Column(Integer, ForeignKey("variable.variable_id"))
        source_id = Column(Integer, ForeignKey("source.source_id"))
        site_id = Column(Integer, ForeignKey("site.site_id"))

    class LabResult(base):
        __tablename__ = "lab_result"
        lab_result_id = Column(Integer, primary_key=True)
        lab_identifier = Column(String)
        variable_id = Column(ForeignKey("variable.variable_id"))
        sample_identifier = Column(String)
        filter_lab = Column(String)
        dilution_lab = Column(REAL)
        method_lab = Column(String)
        value_lab = Column(REAL)
        description_lab = Column(String)
        timestamp_start_lab = Column(DateTime)
        timestamp_end_lab = Column(DateTime)
        site_id = Column(
            ForeignKey("site.site_id"),
        )
        filter_sample = Column(String)
        dilution_sample = Column(REAL)
        timestamp_sample = Column(DateTime)
        method_sample = Column(String)
        description_sample = Column(String)

    class LabResultPersonSampleAssociation(base):
        __tablename__ = "lab_result_person_sample_association"
        lab_result_person_sample_association_id = Column(Integer, primary_key=True)
        lab_result_id = Column(ForeignKey("lab_result.lab_result_id"))
        person_id = Column(ForeignKey("person.person_id"))

    class LabResultPersonLabAssociation(base):
        __tablename__ = "lab_result_person_lab_association"
        lab_result_person_lab_association_id = Column(Integer, primary_key=True)
        lab_result_id = Column(ForeignKey("lab_result.lab_result_id"))
        person_id = Column(ForeignKey("person.person_id"))

    class MetaActionType(base):
        __tablename__ = "meta_action_type"
        meta_action_type_id = Column(Integer, primary_key=True)
        meta_log_type_id = Column(ForeignKey("meta_log_type.meta_log_type_id"))
        name = Column(String)
        description = Column(String)

    class MetaLogType(base):
        __tablename__ = "meta_log_type"
        meta_log_type_id = Column(Integer, primary_key=True)
        name = Column(String)
        description = Column(String)

    class MetaFlag(base):
        __tablename__ = "meta_flag"
        meta_flag_id = Column(Integer, primary_key=True)
        name = Column(String)
        description = Column(String)

    class MetaPicture(base):
        __tablename__ = "meta_picture"
        picture_id = Column(Integer, primary_key=True)
        meta_data_id = Column(ForeignKey("meta_data.meta_data_id"))
        meta_data_history_id = Column(
            ForeignKey("meta_data_history.meta_data_history_id")
        )
        filename = Column(String)
        description = Column(String)
        data = Column(LargeBinary)

    class MetaData(base):
        __tablename__ = "meta_data"
        meta_data_id = Column(Integer, primary_key=True)
        source_id = Column(ForeignKey("source.source_id"))
        site_id = Column(ForeignKey("site.site_id"))
        description = Column(String)
        additional_meta_info = Column(String)

    class MetaDataHistory(base):
        __tablename__ = "meta_data_history"
        meta_data_history_id = Column(Integer, primary_key=True)
        meta_data_id = Column(ForeignKey("meta_data.meta_data_id"))
        meta_log_type_id = Column(ForeignKey("meta_log_type.meta_log_type_id"))
        meta_action_type_id = Column(ForeignKey("meta_action_type.meta_action_type_id"))
        meta_flag_id = Column(ForeignKey("meta_flag.meta_flag_id"))
        person_id = Column(ForeignKey("person.person_id"))
        timestamp_start = Column(DateTime)
        timestamp_end = Column(DateTime)
        comment = Column(String)
        additional_meta_info = Column(String)

    class Person(base):
        __tablename__ = "person"
        person_id = Column(Integer, primary_key=True)
        abbreviation = Column(String)
        name = Column(String)
        email = Column(String)

    class Project(base):
        __tablename__ = "project"
        project_id = Column(Integer, primary_key=True)
        title = Column(String)
        description = Column(String)

    class Quality(base):
        __tablename__ = "quality"
        quality_id = Column(Integer, primary_key=True)
        flag = Column(String)
        method = Column(String)

    class SignalQuality(base):
        __tablename__ = "signal_quality"
        signal_quality_id = Column(Integer, primary_key=True)
        quality_id = Column(ForeignKey("quality.quality_id"))
        timestamp = Column(DateTime)
        author = Column(String)

    class SignalsSignalQualityAssociations(base):
        __tablename__ = "signals_signal_quality_association"
        signals_signal_quality_association_id = Column(Integer, primary_key=True)
        signal_id = Column(ForeignKey("signal.signal_id"))
        signal_quality_id = Column(ForeignKey("signal_quality.signal_quality_id"))

    class Picture(base):
        __tablename__ = "picture"
        picture_id = Column(Integer, primary_key=True)
        site_id = Column(ForeignKey("site.site_id"))
        filename = Column(String, nullable=False)
        description = Column(String)
        timestamp = Column(DateTime)
        data = Column(LargeBinary)

    class SpecialValueDefinition(base):
        __tablename__ = "special_value_definition"
        special_value_definition_id = Column(Integer, primary_key=True)
        source_type_id = Column(ForeignKey("source_type.source_type_id"))
        description = Column(String)
        categorical_value = Column(String)
        numerical_value = Column(REAL)

    base.metadata.create_all(db_engine)

    # IMPORTANT: return order is important for foreign key constraints
    return (
        Site,
        Picture,
        Variable,
        SourceType,
        SpecialValueDefinition,
        Project,
        Source,
        Signal,
        Person,
        BinaryData,
        LabResult,
        LabResultPersonSampleAssociation,
        LabResultPersonLabAssociation,
        MetaLogType,
        MetaActionType,
        MetaFlag,
        MetaPicture,
        MetaData,
        MetaDataHistory,
        Quality,
        SignalQuality,
        SignalsSignalQualityAssociations,
    )
