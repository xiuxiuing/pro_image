from data_mgr_base import DataManagerBase
from data_mgr_import import DataManagerImportMixin
from data_mgr_query import DataManagerQueryMixin
from data_mgr_ops import DataManagerOpsMixin
from data_mgr_export import DataManagerExportMixin
from data_mgr_rule_templates import DataManagerRuleTemplateMixin

class DataManager(DataManagerBase, DataManagerImportMixin, DataManagerQueryMixin, DataManagerOpsMixin, DataManagerExportMixin, DataManagerRuleTemplateMixin):
    """
    DataManager handles the project lifecycle, SQLite database operations, 
    Excel data imports/exports, and state management for the ProImage AI application.
    
    Refactored into multiple mixins to maintain small file sizes for code obfuscation.
    """
    pass
