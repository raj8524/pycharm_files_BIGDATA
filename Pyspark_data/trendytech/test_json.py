from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
from pyspark import SQLContext, HiveContext
import os
import re
import sys
from pyspark.sql.functions import *
import pymssql
from pyspark.sql.types import StringType


def BeginDSIProcessing(datasetId, wfi, eventGroupId):
    STATUS = 'PENDING'
    object_uri = ''
    DSI_BEGIN_PROCESSING_SP = """    SET NOCOUNT ON;
    DECLARE @OUT_DSI BIGINT;
    EXEC  [dbo].[USP_BEGIN_DATASET_INSTANCE_PROCESSING] @dataset_id=%s, @status=%s, @workflow_instance_id=%s, @object_uri=%s, @user_name=%s, @event_group_id=%s, @debug=%s,@dataset_instance_id = @OUT_DSI OUTPUT;
    SELECT @OUT_DSI AS THE_OUTPUT"""
    Param_Values = (datasetId, STATUS, wfi, object_uri, None, eventGroupId, 'N')
    cursor = odbcEngine.cursor()
    cursor.execute(DSI_BEGIN_PROCESSING_SP, Param_Values)
    row = cursor.fetchone()
    OUT_DSI = row[0]
    odbcEngine.commit()
    cursor.close()
    return OUT_DSI


def EndDSIProcessing(workflowInstanceID, datasetInstanceID, dsiStatus, insertedrowcount, eventGroupId):
    DSI_END_PROCESSING_SP = "EXEC [dbo].[USP_END_DATASET_INSTANCE_PROCESSING] @dataset_instance_id=%s, @workflow_instance_id=%s, @dsi_status=%s, @dsi_records_attribute_list=%s, @dsi_records_inserted_list=%s, @dsi_records_updated_list=%s, @dsi_records_deleted_list=%s,@user_name = %s, @event_group_id=%s, @debug = %s;"
    Param_Values = (
    datasetInstanceID, workflowInstanceID, dsiStatus, '<root></root>', insertedrowcount, None, None, None, eventGroupId,
    'N')
    cursor = odbcEngine.cursor()
    cursor.execute(DSI_END_PROCESSING_SP, Param_Values)
    odbcEngine.commit()
    cursor.close()
    return


def flattenDataframe(df):
    fields = df.schema.fields
    available = True
    fieldNames = list(map(lambda x: x.name, fields))
    for field in fields:
        fieldType = field.dataType
        fieldName = field.name
        if isinstance(fieldType, ArrayType):
            fieldNamesExcludingArray = list(filter(lambda fld: fld != field.name, fieldNames))
            fieldNamesExcludingArray.append("explode_outer({0}) as {0}".format(fieldName))
            explodedf = df.selectExpr(*fieldNamesExcludingArray)
            available = True

            return flattenDataframe(explodedf)
        elif isinstance(fieldType, StructType):
            childFieldnames = list((map(lambda childname: fieldName + "." + childname, fieldType.names)))
            if 'Users_Json._corrupt_record' in childFieldnames:
                childFieldnames.remove('Users_Json._corrupt_record')
            newfieldNames = list(filter(lambda fld: fld != fieldName, fieldNames)) + list(childFieldnames)
            renamedcols = list(map(lambda x: "{0} as {1}".format(x, x.replace(".", "_")), newfieldNames))
            explodedf = df.selectExpr(*renamedcols)
            available = True
            return flattenDataframe(explodedf)
        else:
            available = False
    if not available:
        return df


def captureMissingKeys(df):
    fields = df.schema.fields
    knownFields = ['Id', 'Email', 'Mobile', 'FirstName', 'LastName', 'PasswordHash', 'EmailConfirmationKey',
                   'MobileConfirmationKey', 'EmailConfirmationExpiration',
                   'MobileConfirmationExpiration', 'IsEmailConfirmed', 'IsMobileConfirmed', 'PasswordResetKey',
                   'PasswordResetExpiration', 'LastLogin',
                   'RegistrationCountry', 'MacAddress', 'Birthday', 'Gender', 'ActivationDate', 'FacebookUserId',
                   'GoogleUserId', 'TwitterUserId', 'B2BUserId',
                   'CreationDate', 'System', 'NewEmail', 'NewEmailConfirmationKey', 'NewEmailConfirmationExpiration',
                   'State', 'IpAddress', 'RegistrationRegion', 'Json',
                   'AmazonUserId', 'ProviderName', 'ProviderSubjectId', 'Users_Json_Source', 'Users_Json_Source_1',
                   'Users_Json_Source_1app', 'Users_Json_True_Client_IP',
                   'Users_Json_X_Forwarded_For', 'Users_Json__corrupt_record', 'Users_Json_additionalProp1',
                   'Users_Json_additionalProp2', 'Users_Json_additionalProp3',
                   'Users_Json_advertising_id', 'Users_Json_dsfasdf', 'Users_Json_first_time_login',
                   'Users_Json_free_trial', 'Users_Json_gdpr_policy',
                   'Users_Json_guest_token', 'Users_Json_original_user_agent', 'Users_Json_partner', 'Users_Json_pas',
                   'Users_Json_paymentmode',
                   'Users_Json_recurring_enabled', 'Users_Json_siti_sub_id', 'Users_Json_user_type',
                   'Users_Json_version_number', 'Users_Json_zee5_integration_partner',
                   'Users_Json_campaign_TV', 'Users_Json_campaign_TV_',
                   'Users_Json_campaign_Telugu_eq_branding_20190124_to_20190331_tag1_svod',
                   'Users_Json_campaign_af_status', 'Users_Json_campaign_affid', 'Users_Json_campaign_campaign',
                   'Users_Json_campaign_campaign_name',
                   'Users_Json_campaign_click_time', 'Users_Json_campaign_cmpid', 'Users_Json_campaign_code',
                   'Users_Json_campaign_cost_cents_USD',
                   'Users_Json_campaign_dclid', 'Users_Json_campaign_fbclid', 'Users_Json_campaign_gclid',
                   'Users_Json_campaign_install_time',
                   'Users_Json_campaign_is_cache', 'Users_Json_campaign_is_first_launch', 'Users_Json_campaign_iscache',
                   'Users_Json_campaign_lead',
                   'Users_Json_campaign_media_Source_1', 'Users_Json_campaign_ompAff', 'Users_Json_campaign_orig_cost',
                   'Users_Json_campaign_qg_id',
                   'Users_Json_campaign_simmba_regional_eq_branding_20190423_to_20190531_tag1_svod',
                   'Users_Json_campaign_st', 'Users_Json_campaign_u',
                   'Users_Json_campaign_utm_Source_1', 'Users_Json_campaign_utm_campaign',
                   'Users_Json_campaign_utm_content', 'Users_Json_campaign_utm_medium',
                   'Users_Json_campaign_utm_term', 'Users_Json_campaign_utm_trailer',
                   'Users_Json_campaign_zee5_integration_partner',
                   'Users_Json_campaign_zee5_integration_type', 'Users_Json_gdpr_fields_age',
                   'Users_Json_gdpr_fields_policy', 'Users_Json_gdpr_fields_profiling',
                   'Users_Json_gdpr_fields_subscription', 'Users_Json_promotional_on', 'Users_Json_promotional_token',
                   'Users_Json_promotional_platform_amazonfire_tv',
                   'Users_Json_promotional_platform_android_mobile', 'Users_Json_promotional_platform_xiaomi_tv',
                   'Users_Json_gdpr_AE_field', 'Users_Json_gdpr_AF_field',
                   'Users_Json_gdpr_AL_field', 'Users_Json_gdpr_AU_field', 'Users_Json_gdpr_DE_field',
                   'Users_Json_gdpr_GB_field', 'Users_Json_gdpr_IN_field',
                   'Users_Json_gdprPolicy_country_code', 'Users_Json_additional_gdpr_policy_country_code',
                   'Users_Json_gdprPolicy_gdpr_fields_age',
                   'Users_Json_gdprPolicy_gdpr_fields_policy', 'Users_Json_gdprPolicy_gdpr_fields_profiling',
                   'Users_Json_gdprPolicy_gdpr_fields_subscription',
                   'Users_Json_additional_gdpr_policy_gdpr_fields_age',
                   'Users_Json_additional_gdpr_policy_gdpr_fields_policy',
                   'Users_Json_additional_gdpr_policy_gdpr_fields_profiling',
                   'Users_Json_additional_gdpr_policy_gdpr_fields_subscription',
                   'Users_Json_campaign_utm_campaign_1857_7c',
                   'Users_Json_campaign_http__www_zee5_com_myaccount_subscription',
                   'Users_Json_campaign_https_www_zee5_com_myaccount_subscription', 'Users_Json_campaign_utm_mediaum',
                   'Users_Json_field', 'Users_Json_gdpr', 'Users_Json_gdpr_policy_country_code',
                   'Users_Json_gdpr_policy_gdpr_fields', 'Users_Json_gdpr_policy_gdpr_fields_age',
                   'Users_Json_gdpr_policy_gdpr_fields_policy', 'Users_Json_gdpr_policy_gdpr_fields_profiling',
                   'Users_Json_gdpr_policy_gdpr_fields_subscription', 'Users_Json_gdpr_policy_Source_1app',
                   'Users_Json_gender', 'Users_Json_gender1', 'Users_Json_utm_data', 'Users_Json_utm_data_utm_campaign',
                   'Users_Json_utm_data_utm_medium', 'Users_Json_utm_data_utm_Source_1',
                   'Users_Json_campaign_is_retargeting', 'Users_Json_campaign_Lapsers_Aug_Tenth',
                   'Users_Json_campaign_Male_Audience_Subscribe_Now_July_thirteen', 'Users_Json_campaign_msclkid',
                   'Users_Json_campaign_name', 'Users_Json_campaign_pid', 'Users_Json_campaign_shortlink',
                   'Users_Json_campaign_Source_1_attribution', 'Users_Json_campaign_tagtag_uid',
                   'Users_Json_additionalProp1_country_code', 'Users_Json_additionalProp1_gdpr_fields_age',
                   'Users_Json_additionalProp1_gdpr_fields_policy', 'Users_Json_additionalProp1_gdpr_fields_profiling',
                   'Users_Json_additionalProp1_gdpr_fields_subscription', 'Users_Json_birthday', 'Users_Json_campaign',
                   'Users_Json_campaign_af_deeplink', 'Users_Json_campaign_af_web_id', 'Users_Json_campaign_autoapply',
                   'Users_Json_campaign_c', 'Users_Json_campaign_ChickenMasala_Promotions',
                   'Users_Json_campaign_gclsrc', 'Users_Json_campaign_utm_id']
    missing_keys = []

    for field in fields:
        fieldName = field.name
        if str(fieldName) not in knownFields:
            missing_keys.append(fieldName)
    return missing_keys


def missingKeyCapture(keys):
    source = 'Users'
    keyslist = ','.join(keys)
    print(keyslist)
    Execute_MISSING_CAPTURE_KEY = "EXEC [dbo].[usp_add_new_json_keys] @source=%s,@keyslist=%s;"
    print(source)
    print(keys)
    Param_Values = (source, keyslist)
    cursor = odbcEngine.cursor()
    cursor.execute(Execute_MISSING_CAPTURE_KEY, Param_Values)
    odbcEngine.commit()
    cursor.close()
    return


spark = SparkSession.builder.enableHiveSupport().getOrCreate()
hiveContext = HiveContext(spark.sparkContext)

accessKey = "AKIA5K35V3MBQMFSZD4X"
secretKey = "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"
filepath = "s3://zee5-axinom-data-prod/Inbound/Users_17032022_001116.csv"
flattenedpath = "s3://zee5-axinom-data-prod/AXINOM_USERS_FLATTENED/"
WFI_ID = "248749"
server = "10.2.0.47"
db = "nostradamusprod_metastore"
pwd = "seQaf4A6"
port_num = "1433"
uname = "_oozie"
stagetablename = "nostradamusprod_staging.stg_axinomusers"
ext_stagetablename = "nostradamusprod_staging.STG_AXINOM_USERS_EXTERNAL"
output_dataset_id = "15000000010"
event_group_id = "248813"
# sqlQuery_0 = ''
sqlQuery_1 = ''
sqlQuery_2 = ''
sqlQuery_3 = ''

try:
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", accessKey)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secretKey)
    ##  spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.hive.metastore.warehouse.dir", "s3://cdp-nostradamus/warehouse")
    odbcEngine = pymssql.connect(host=server, database=db, user=uname, password=pwd, port=port_num)
    OUT_DSI = int(BeginDSIProcessing(output_dataset_id, WFI_ID, event_group_id))
    Users_DF = spark.read.option('header', 'true').option('delimiter', '|').csv(filepath)
    Users_DF = Users_DF.withColumn('Json', regexp_replace('Json', 'AppVersion=2.0.1": "" }', 'AppVersion=2.0.1"}'))
    Users_DF = Users_DF.withColumn('Json', regexp_replace('Json', 'AppVersion=2.0.2": "" }', 'AppVersion=2.0.2"}'))
    Users_DF = Users_DF.withColumn('Json', regexp_replace('Json', 'source', 'Source'))
    ##Users_DF = spark.read.option('header','true').option('delimiter','|').csv('s3://zee5-axinom-data-prod/Inbound/Users_21012020_010030.csv')
    Users_DF = Users_DF.withColumn('Json', regexp_replace('Json', 'tcProfile', 'tcprofile'))
    Users_Schema = spark.read.json(Users_DF.rdd.map(lambda row: row.Json)).schema
    UsersJSON_DF = Users_DF.withColumn('Users_Json', from_json('Json', Users_Schema))
    flattenedDF = flattenDataframe(UsersJSON_DF)
    # gdpr_Schema = spark.read.json(flattenedDF.rdd.map(lambda row: row.Users_Json_gdpr_policy_gdpr_fields)).schema
    # gdpr_JSON_DF = flattenedDF.withColumn('Users_Json_gdpr_policy_gdpr_fields',from_json('Users_Json_gdpr_policy_gdpr_fields',gdpr_Schema))
    # flattenedDF = flattenDataframe(gdpr_JSON_DF)
    # flattenedDF.drop('users_json__corrupt_record')
    # utm_Schema = spark.read.json(flattenedDF.rdd.map(lambda row: row.Users_Json_utm_data)).schema
    # utm_JSON_DF = flattenedDF.withColumn('Users_Json_utm_data',from_json('Users_Json_utm_data',utm_Schema))
    # flattenedDF = flattenDataframe(utm_JSON_DF)
    # flattenedPolicyDF = flattenDataframe(flattenedDF.select(col('Users_Json_gdpr_policy')))
    # flattenedPolicyDF.filter("id='2759607B-B978-42C4-8015-0C4173330789'").select(col('Users_Json_gdprPolicy_gdpr_fields_age')).show()
    lstMissingKeys = captureMissingKeys(flattenedDF)
    flattenedDF = flattenedDF.withColumn('Keys', lit(''))
    # The below column keeps getting resolved to BIGINT/BINARY randomly causing DATATYPE issues when reading PARQUET file data. Casting datatype to STRING to prevent issues
    # if flattenedDF.columns.contains("Users_Json_campaign_click_time") == True:

    if 'Users_Json_campaign_click_time' in flattenedDF.columns:
        flattenedDF = flattenedDF.withColumn("Users_Json_campaign_click_time",
                                             flattenedDF.Users_Json_campaign_click_time.cast(StringType()))

    for lst in lstMissingKeys:
        key = str(lst)
        flattenedDF = flattenedDF.withColumn('Keys', when(col(lst).isNotNull(), 'Key is missing').otherwise(''))
        flattenedDF = flattenedDF.drop(lst)

    ##  flattenedDF  = flattenedDF.withColumn('dataset_instance_id',lit(OUT_DSI))
    flattenedDF.write.parquet(flattenedpath + 'pc_dataset_instance_id=' + str(OUT_DSI))
    print("Flattening is completed...")

    #    sqlQuery_0 = "drop table {0}".format(ext_stagetablename)
    #    sqlQuery_1 = "create table {0} using PARQUET location '{1}'".format(ext_stagetablename,flattenedpath+'pc_dataset_instance_id='+str(OUT_DSI))
    sqlQuery_1 = "Alter table {0} set location '{1}'".format(ext_stagetablename,
                                                             flattenedpath + 'pc_dataset_instance_id=' + str(OUT_DSI))
    sqlQuery_2 = "insert into {1} partition(pc_dataset_instance_id={0}) Select {0} as dataset_instance_id,Id,Email,Mobile,FirstName,LastName,PasswordHash,EmailConfirmationKey,MobileConfirmationKey,EmailConfirmationExpiration,MobileConfirmationExpiration,IsEmailConfirmed,IsMobileConfirmed,PasswordResetKey,PasswordResetExpiration,LastLogin,RegistrationCountry,MacAddress,Birthday,Gender,ActivationDate,FacebookUserId,GoogleUserId,TwitterUserId,B2BUserId,CreationDate,System,NewEmail,NewEmailConfirmationKey,NewEmailConfirmationExpiration,State,IpAddress,RegistrationRegion,Json,AmazonUserId,ProviderName,ProviderSubjectId,Users_Json_Source,Users_Json_Source_1,Users_Json_Source_1app,Users_Json_True_Client_IP,Users_Json_X_Forwarded_For,Users_Json__corrupt_record,Users_Json_additionalProp1,Users_Json_additionalProp2,Users_Json_additionalProp3,Users_Json_advertising_id,Users_Json_dsfasdf,Users_Json_first_time_login,Users_Json_free_trial,Users_Json_gdpr_policy,Users_Json_guest_token,Users_Json_original_user_agent,Users_Json_partner,Users_Json_pas,Users_Json_paymentmode,Users_Json_recurring_enabled,Users_Json_siti_sub_id,Users_Json_user_type,Users_Json_version_number,Users_Json_zee5_integration_partner,Users_Json_campaign_TV,Users_Json_campaign_TV_,Users_Json_campaign_Telugu_eq_branding_20190124_to_20190331_tag1_svod,Users_Json_campaign_af_status,Users_Json_campaign_affid,Users_Json_campaign_campaign,Users_Json_campaign_campaign_name,Users_Json_campaign_click_time,Users_Json_campaign_cmpid,Users_Json_campaign_code,Users_Json_campaign_cost_cents_USD,Users_Json_campaign_dclid,Users_Json_campaign_fbclid,Users_Json_campaign_gclid,Users_Json_campaign_install_time,Users_Json_campaign_is_cache,Users_Json_campaign_is_first_launch,Users_Json_campaign_iscache,Users_Json_campaign_lead,Users_Json_campaign_media_Source_1,Users_Json_campaign_ompAff,Users_Json_campaign_orig_cost,Users_Json_campaign_qg_id,Users_Json_campaign_simmba_regional_eq_branding_20190423_to_20190531_tag1_svod,Users_Json_campaign_st,Users_Json_campaign_u,Users_Json_campaign_utm_Source_1,Users_Json_campaign_utm_campaign,Users_Json_campaign_utm_content,Users_Json_campaign_utm_medium,Users_Json_campaign_utm_term,Users_Json_campaign_utm_trailer,Users_Json_campaign_zee5_integration_partner,Users_Json_campaign_zee5_integration_type,Users_Json_gdpr_fields_age,Users_Json_gdpr_fields_policy,Users_Json_gdpr_fields_profiling,Users_Json_gdpr_fields_subscription,Users_Json_promotional_on,Users_Json_promotional_token,Users_Json_promotional_platform_amazonfire_tv,Users_Json_promotional_platform_android_mobile,Users_Json_promotional_platform_xiaomi_tv,Users_Json_gdpr_AE_field,Users_Json_gdpr_AF_field,Users_Json_gdpr_AL_field,Users_Json_gdpr_AU_field,Users_Json_gdpr_DE_field,Users_Json_gdpr_GB_field,Users_Json_gdpr_IN_field,Users_Json_gdprPolicy_country_code,Users_Json_additional_gdpr_policy_country_code,Users_Json_gdprPolicy_gdpr_fields_age,Users_Json_gdprPolicy_gdpr_fields_policy,Users_Json_gdprPolicy_gdpr_fields_profiling,Users_Json_gdprPolicy_gdpr_fields_subscription,Users_Json_additional_gdpr_policy_gdpr_fields_age,Users_Json_additional_gdpr_policy_gdpr_fields_policy,Users_Json_additional_gdpr_policy_gdpr_fields_profiling,Users_Json_additional_gdpr_policy_gdpr_fields_subscription,Users_Json_campaign_utm_campaign_1857_7c ,Users_Json_campaign_http__www_zee5_com_myaccount_subscription ,Users_Json_campaign_https_www_zee5_com_myaccount_subscription ,Users_Json_campaign_utm_mediaum ,Users_Json_field ,Users_Json_gdpr ,Users_Json_gdpr_policy_country_code ,Users_Json_gdpr_policy_gdpr_fields ,Users_Json_gdpr_policy_gdpr_fields_age ,Users_Json_gdpr_policy_gdpr_fields_policy ,Users_Json_gdpr_policy_gdpr_fields_profiling ,Users_Json_gdpr_policy_gdpr_fields_subscription ,Users_Json_gdpr_policy_Source_1app ,Users_Json_gender ,Users_Json_gender1 ,Users_Json_utm_data ,Users_Json_utm_data_utm_campaign ,Users_Json_utm_data_utm_medium ,Users_Json_utm_data_utm_Source_1 ,Users_Json_campaign_is_retargeting ,Users_Json_campaign_Lapsers_Aug_Tenth ,Users_Json_campaign_Male_Audience_Subscribe_Now_July_thirteen ,Users_Json_campaign_msclkid ,Users_Json_campaign_name ,Users_Json_campaign_pid ,Users_Json_campaign_shortlink ,Users_Json_campaign_Source_1_attribution ,Users_Json_campaign_tagtag_uid ,Users_Json_additionalProp1_country_code ,Users_Json_additionalProp1_gdpr_fields_age ,Users_Json_additionalProp1_gdpr_fields_policy ,Users_Json_additionalProp1_gdpr_fields_profiling ,Users_Json_additionalProp1_gdpr_fields_subscription ,Users_Json_birthday ,Users_Json_campaign ,Users_Json_campaign_af_deeplink ,Users_Json_campaign_af_web_id ,Users_Json_campaign_autoapply ,Users_Json_campaign_c ,Users_Json_campaign_ChickenMasala_Promotions ,Users_Json_campaign_gclsrc ,Users_Json_campaign_utm_id,keys from {2}".format(
        OUT_DSI, stagetablename, ext_stagetablename)
    sqlQuery_3 = "Select count(1) as insertedrowcount from {1} where pc_dataset_instance_id={0}".format(OUT_DSI,
                                                                                                        stagetablename)

    #    print(sqlQuery_0)
    print(sqlQuery_1)
    print(sqlQuery_2)
    print(sqlQuery_3)

    #    hiveContext.sql(sqlQuery_0)
    hiveContext.sql(sqlQuery_1)
    hiveContext.sql(sqlQuery_2)
    insertedrecordcount = int(hiveContext.sql(sqlQuery_3).take(1)[0].__getitem__(0))
    print("Staging is completed...")
    EndDSIProcessing(WFI_ID, OUT_DSI, "READY", insertedrecordcount, event_group_id)
    missingKeyCapture(lstMissingKeys)
except Exception as e:
    print(e)
    sys.exit(-1)
spark.stop()

