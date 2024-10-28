# check_blob_query = '''
# select
#     tc.OWNER
#     , tc.TABLE_NAME
#     , tc.DATA_TYPE
# from all_tab_cols tc
# where tc.table_name = UPPER(:TABLENAME)
#     and tc.column_name = UPPER(:COLUMNNAME)
#     and tc.owner = UPPER(:OWNER)
# '''
#
# get_blob_image = '''
# select mt.icon from morion.trademark mt where mt.name = 'TEST'
# '''
