from configurations import *
from specification_builder import SpecificationBuilder

# target_table_list = ['we_user', 'wv_order', 'ws_order', 'ws_album', 'we_artist']


# for target_table in target_table_list:
target_table = 'wv_dm_subscr_daily'

spec_builder = SpecificationBuilder(target_table=target_table)
spec_builder.collect_static_data()
spec_builder.build_mdfile()