# -*- coding: utf-8 -*-
"""
Created on Tue Nov  1 09:57:04 2022

@author: breadsp2
"""
import aws_creds_prod
import boto3
import pandas as pd
from pandas_aws import s3 as pd_s3
import sqlalchemy as sd
import re
from dateutil.parser import parse
import datetime
import os

def validate_accrual():
    pd.options.mode.chained_assignment = None
    s3_client = boto3.client('s3', aws_access_key_id=aws_creds_prod.aws_access_id, aws_secret_access_key=aws_creds_prod.aws_secret_key,
                             region_name='us-east-1')
    bucket_name = "nci-cbiit-seronet-submissions-passed"
    cbc_name_list = {"Feinstein_CBC01" : 41, "UMN_CBC02": 27, "ASU_CBC03": 32, "Mt_Sinai_CBC04" :14}
    output_file_dir = r"C:\Users\breadsp2\Desktop\Accrual Errors"
    
    engine, conn = connect_to_sql_db(sd, "seronetdb-Vaccine_Response")

    for curr_cbc in cbc_name_list:
        #if curr_cbc not in ["UMN_CBC02"]:
        #    continue

        file_path = f"Monthly_Accrual_Reports/{curr_cbc}/"
        writer = pd.ExcelWriter(output_file_dir + os.path.sep + curr_cbc + "_monthly_accrual_error_file.xlsx", engine='xlsxwriter')

        acc_participant_data = pd_s3.get_df_from_keys(s3_client, bucket_name, file_path, suffix="Accrual_Participant_Info.csv", format="csv",
                                                      na_filter=False, output_type="pandas")
        acc_visit_data = pd_s3.get_df_from_keys(s3_client, bucket_name, file_path, suffix="Accrual_Visit_Info.csv", format="csv",
                                                na_filter=False, output_type="pandas")
        acc_vaccine_data = pd_s3.get_df_from_keys(s3_client, bucket_name, file_path, suffix="Accrual_Vaccination_Status.csv", format="csv",
                                                  na_filter=False, output_type="pandas")

        acc_participant_data.rename(columns = {"Week_Of_Visit_1": "Sunday_Prior_To_Visit_1"}, inplace = True)
        acc_visit_data.rename(columns={'Collected_in_This_Reporting_Period': 'Collected_In_This_Reporting_Period'}, inplace=True)
        acc_visit_data.replace("Baseline(1)", 1, inplace=True)
        acc_vaccine_data.rename(columns={'Visit_Date_Duration_From_Visit_1': 'SARS-CoV-2_Vaccination_Date_Duration_From_Visit1'}, inplace=True)
        acc_vaccine_data.replace("Baseline(1)", 1, inplace=True)

        acc_participant_data = clean_up_data(acc_participant_data)
        acc_participant_data.drop_duplicates(["Research_Participant_ID", "Age"], inplace=True, keep='last')
        acc_participant_data.reset_index(drop=True, inplace=True)
        
        acc_visit_data = clean_up_data(acc_visit_data)
        acc_visit_data.drop_duplicates(['Research_Participant_ID', 'Visit_Number'], inplace=True, keep='last')
        acc_visit_data.reset_index(drop=True, inplace=True)

        acc_vaccine_data = clean_up_data(acc_vaccine_data)
        acc_vaccine_data.drop_duplicates(inplace=True, keep='last')
        acc_vaccine_data.reset_index(drop=True, inplace=True)

        check_part = acc_participant_data.merge(acc_visit_data, on="Research_Participant_ID", how="outer", indicator="Part_Visit")
        missing_part = check_part.query("Part_Visit in ['right_only']")   # flags data were participant not in visit or vise vera
        missing_visit = check_part.query("Part_Visit in ['left_only']")   # flags data were participant not in visit or vise vera
        part_errors = pd.DataFrame({'Column_Name':  "Research_Participant_ID", 'Column_Value': missing_part["Research_Participant_ID"],
                                    'Error_Message': "Participant_ID has Visit Data but no Demographics"})
        visit_errors = pd.DataFrame({'Column_Name':  "Research_Participant_ID", 'Column_Value': missing_visit["Research_Participant_ID"],
                                     'Error_Message': "Participant_ID has Demographic Info, but no Visit Data"})

        if len(part_errors) > 0:
            acc_visit_data = acc_visit_data.query("Research_Participant_ID not in {0}".format(part_errors["Column_Value"].tolist()))
            acc_vaccine_data = acc_vaccine_data.query("Research_Participant_ID not in {0}".format(part_errors["Column_Value"].tolist()))
        if len(visit_errors) > 0:
             acc_participant_data =  acc_participant_data.query("Research_Participant_ID not in {0}".format(visit_errors["Column_Value"].tolist()))
        demo_data = pd.concat([part_errors, visit_errors])
        demo_data.to_excel(writer, index=False, sheet_name="Participant ID Errors")

        check_visit = acc_visit_data.merge(acc_vaccine_data, on=["Research_Participant_ID","Visit_Number"], how="outer", indicator="Part_Vacc")
        visit_errors = check_visit.query("Part_Vacc in ['left_only']")   # flags data were participant not in visit or vise vera
        vacc_errors = check_visit.query("Part_Vacc in ['right_only']")   # flags data were participant not in visit or vise vera
        
        visit_errors = pd.DataFrame({"Research_Participant_ID": visit_errors["Research_Participant_ID"], "Visit_Num": visit_errors["Visit_Number"],
                                    'Error_Message': "Participant has a Visit in Visit Data, but missing coresponding visit in Accrual_Vaccination_Status.csv"})
        vacc_errors = pd.DataFrame({"Research_Participant_ID": vacc_errors["Research_Participant_ID"], "Visit_Num": vacc_errors["Visit_Number"],
                                    'Error_Message': "Participant has a visit in vaccination history, but missing coresponding visit in Visit_Info.csv"})

        visit_data = pd.concat([visit_errors, vacc_errors])
        visit_data.to_excel(writer, index=False, sheet_name="Visit Data Errors")
        
        if len(visit_errors) > 0:
            acc_visit_data["combo"] = list(zip(acc_visit_data["Research_Participant_ID"], acc_visit_data["Visit_Number"]))
            error_list = list(zip(visit_errors["Research_Participant_ID"], visit_errors["Visit_Num"]))
            acc_visit_data = acc_visit_data[acc_visit_data["combo"].apply(lambda x: x not in error_list)]
            acc_visit_data.drop("combo", axis=1, inplace=True)
        if len(vacc_errors) > 0:
            acc_vaccine_data["combo"] = list(zip(acc_vaccine_data["Research_Participant_ID"], acc_vaccine_data["Visit_Number"]))
            error_list = list(zip(vacc_errors["Research_Participant_ID"], vacc_errors["Visit_Num"]))
            acc_vaccine_data = acc_vaccine_data[acc_vaccine_data["combo"].apply(lambda x: x not in error_list)]
            acc_vaccine_data.drop("combo", axis=1, inplace=True)
        try:
            part_errors = check_part_rules(acc_participant_data, cbc_name_list[curr_cbc])
            visit_errors = check_visit_rules(acc_visit_data, cbc_name_list[curr_cbc])
            vaccine_errors = check_vaccine_rules(acc_vaccine_data, cbc_name_list[curr_cbc])

            part_errors["Sheet_Name"] = "Accrual_Participant_Info.csv"
            visit_errors["Sheet_Name"] = "Accrual_Visit_Info.csv"
            vaccine_errors["Sheet_Name"] = "Accrual_Vaccination_Status.csv"
            
            if len(part_errors) > 0:
                part_errors.drop_duplicates(["Column_Name","Column_Value"], inplace=True)
                part_errors.reset_index(inplace=True)
                for x in part_errors.index:
                    acc_participant_data = acc_participant_data.query("`{0}` != '{1}'".format(part_errors.loc[x]["Column_Name"],part_errors.loc[x]["Column_Value"]))
                acc_visit_data = acc_visit_data.merge(acc_participant_data["Research_Participant_ID"])
                acc_vaccine_data = acc_vaccine_data.merge(acc_participant_data["Research_Participant_ID"])
            elif len(visit_errors) > 0:
                print("x")
            elif len(vaccine_errors) > 0:
                print("y")
                
            all_errors = pd.concat([part_errors, visit_errors, vaccine_errors])
            all_errors["Error Count"] = 0
            all_errors = all_errors.groupby(['Column_Name', 'Column_Value', 'Error_Message', 'Sheet_Name'], as_index=False).count()
            all_errors.to_excel(writer, index=False, sheet_name="Sheet Data Errors")
            acc_visit_data["Normalized_Visit_Number"] = 0

            uni_part = list(set(acc_visit_data["Research_Participant_ID"]))
            for curr_id in uni_part:
                x = acc_visit_data.query("Research_Participant_ID == @curr_id")
                x = x.sort_values("Visit_Date_Duration_From_Visit_1")
                x["Normalized_Visit_Number"] = list(range(1,len(x)+1))
                acc_visit_data.loc[x.index, "Normalized_Visit_Number"] = x["Normalized_Visit_Number"]
            if len(all_errors) == 0:
                print(f"No errors were found for {curr_cbc}")
                # no longer need this part as bruce's code does this already
                #upload_data(acc_participant_data, "Accrual_Participant_Info", engine, conn, ["Age"])
                #upload_data(acc_visit_data, "Accrual_Visit_Info", engine, conn, ["Visit_Number"])
                #upload_data(acc_vaccine_data, "Accrual_Vaccination_Status", engine, conn, ["Visit_Number", "Vaccination_Status", "SARS-CoV-2_Vaccine_Type"])
            else:
                print(f"errors were found for {curr_cbc}: {len(all_errors)}")
        except Exception as e:
            print(e)
    writer.save()
    writer.handles = None


def clean_up_data(df):
    for curr_col in df.columns:
        df[curr_col] = [convert_data_type(c) for c in df[curr_col]]
    return df


def get_s3_folders(s3, pd, bucket, cbc_folder, suffix):
    sub_folders = "Monthly_Accrual_Reports/"
    new_prefix = sub_folders + cbc_folder
    key_list = []
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=new_prefix)
    if 'Contents' in resp:
        for obj in resp['Contents']:
            key = obj['Key']
            if key.endswith(suffix):
                key_list.append(key)
    new_list = [i.split("/") for i in key_list]
    if len(new_list) == 0:
        return ""
    else:
        z = pd.DataFrame(new_list, columns=["Accrual_Folder", "CBC_Name", "S3_Date", "File_Name", "File_Folder", "Submission_File"])
        z.sort_values(by="S3_Date", ascending=False, inplace=True)
        z = z.iloc[0]   #sort by date in decending order then take first (get latest submission)
        return f"{z['Accrual_Folder']}/{z['CBC_Name']}/{z['S3_Date']}/{z['File_Name']}/{z['File_Folder']}"


def convert_data_type(v):
    if isinstance(v, (datetime.datetime, datetime.time, datetime.date)):
        return v
    if str(v).find('_') > 0:
        return v
    try:
        float(v)
        if (float(v) * 10) % 10 == 0:
            return int(float(v))
        return float(v)
    except ValueError:
        try:
            v = parse(v)
            return v
        except ValueError:
            return v
        except TypeError:
            str(v)

def add_df_cols(df_name, field_name, error_msg):
    df_name["Error_Message"] = "None"
    df_name["Column_Name"] = field_name
    if len(df_name) > 0:
        df_name["Error_Message"] = error_msg
    df_name = df_name[["Column_Name", field_name, "Error_Message"]]
    return df_name


def check_part_rules(participant_data, cbc_id):
    error_table = pd.DataFrame(columns=["Column_Name", "Column_Value", "Error_Message"])
    for curr_col in participant_data.columns:
        participant_data[curr_col] = [convert_data_type(c) for c in participant_data[curr_col]]
        if curr_col == "Research_Participant_ID":
            pattern_str = '[_]{1}[A-Z, 0-9]{6}$'
            error_table = pd.concat([error_table, check_id_field(participant_data, re, curr_col, pattern_str, cbc_id, "XX_XXXXXX")])
        if curr_col == "Age":
            error_table = pd.concat([error_table,check_is_number(participant_data, curr_col, 1, 90)])
        if (curr_col in ['Race', 'Ethnicity', 'Gender', 'Sex_At_Birth']):
            if (curr_col in ['Race']):
                list_values = ['White', 'American Indian or Alaska Native', 'Black or African American', 'Asian',
                               'Native Hawaiian or Other Pacific Islander', 'Other', 'Multirace', 'Unknown']  # removing 'Not Reported'
            elif (curr_col in ['Ethnicity']):
                list_values = ['Hispanic or Latino', 'Not Hispanic or Latino', 'Unknown',  'Not Reported']
            elif (curr_col in ['Gender', 'Sex_At_Birth']):
                list_values = ['Male', 'Female', 'InterSex', 'Not Reported', 'Prefer Not to Answer', 'Unknown', 'Other']
            error_table = pd.concat([error_table, check_if_list(participant_data, curr_col, list_values)])
        if curr_col in "Week_Of_Visit_1":
            error_table = pd.concat([check_if_date(participant_data, curr_col)])
    return error_table


def check_visit_rules(visit_data, cbc_id):
    error_table = pd.DataFrame(columns=["Column_Name", "Column_Value", "Error_Message"])
    for curr_col in visit_data.columns:
        visit_data[curr_col] = [convert_data_type(c) for c in visit_data[curr_col]]
        if curr_col == "Research_Participant_ID":
            pattern_str = '[_]{1}[A-Z, 0-9]{6}$'
            error_table = pd.concat([error_table, check_id_field(visit_data, re, curr_col, pattern_str, cbc_id, "XX_XXXXXX")])
        if (curr_col in ['Primary_Cohort', 'SARS_CoV_2_Infection_Status', 'Unscheduled_Visit', 'Unscheduled_Visit_Purpose',
                         'Lost_To_FollowUp', 'Final_Visit', 'Collected_In_This_Reporting_Period', 'Visit_Number', 'Serum_Shipped_To_FNL', 'PBMC_Shipped_To_FNL']):
            if curr_col in ['Primary_Cohort']:
                list_values = ['Autoimmune', 'Cancer', 'Healthy Control','HIV', 'IBD', 'Pediatric', 'Transplant', 'PRIORITY', 'Chronic Conditions']
            if curr_col in ['SARS_CoV_2_Infection_Status']:
                list_values = ['Has Reported Infection', 'Has Not Reported Infection', 'Not Reported']
            if curr_col in ['Unscheduled_Visit', 'Final_Visit', 'Lost_To_FollowUp']:
                list_values = ['Yes', 'No', 'Unknown']
            if curr_col in ['Collected_In_This_Reporting_Period']:
                list_values = ['Yes', 'No']
            if curr_col in ['Unscheduled_Visit_Purpose']:
                list_values = ['Breakthrough COVID', 'Completion of Primary Vaccination Series', 'Completion of Booster', 'Other', 'N/A']
            if curr_col in ['Serum_Shipped_To_FNL', 'PBMC_Shipped_To_FNL']:
                list_values = ['Yes', 'No', 'N/A']
            if (curr_col in ['Visit_Number']):
                list_values = ["Baseline(1)"] + [str(i) for i in list(range(1,30))] + [i for i in list(range(1,30))]
            error_table = pd.concat([error_table, check_if_list(visit_data, curr_col, list_values)])
        if (curr_col in ['Visit_Date_Duration_From_Visit_1']):
             error_table = pd.concat([error_table,check_is_number(visit_data, curr_col, -1000, 1000, NA_Allowed=False)])
        if (curr_col in ['Serum_Volume_For_FNL', 'PBMC_Concentration', 'Num_PBMC_Vials_For_FNL']):
            error_table = pd.concat([error_table,check_is_number(visit_data, curr_col, -1, 1e9, NA_Allowed=True)])
    return error_table


def check_vaccine_rules(vaccine_data, cbc_id):
    error_table = pd.DataFrame(columns=["Column_Name", "Column_Value", "Error_Message"])
    for curr_col in vaccine_data.columns:
        vaccine_data[curr_col] = [convert_data_type(c) for c in vaccine_data[curr_col]]
        if curr_col == "Research_Participant_ID":
            pattern_str = '[_]{1}[A-Z, 0-9]{6}$'
            error_table = pd.concat([error_table, check_id_field(vaccine_data, re, curr_col, pattern_str, cbc_id, "XX_XXXXXX")])

        if (curr_col in ['Visit_Number', 'Vaccination_Status', 'SARS-CoV-2_Vaccine_Type']):
            if curr_col in ['Vaccination_Status']:
                list_values = (['Unvaccinated', 'No vaccination event reported', 'Dose 1 of 1', 'Dose 1 of 2', 'Dose 2 of 2', 'Dose 2', 'Dose 3', 'Dose 4'] +
                              ["Booster " + str(i) for i in list(range(1,10))] + 
                              ["Booster " + str(i) + ":Bivalent" for i in list(range(1,10))])
            if curr_col in ['SARS-CoV-2_Vaccine_Type']:
                list_values = ['Johnson & Johnson', 'Moderna', 'Pfizer', 'Unknown', 'N/A', 'Janssen', 'Sputnik V']
            if (curr_col in ['Visit_Number']):
                list_values = ["Baseline(1)"] + [str(i) for i in list(range(1,30))] + [i for i in list(range(1,30))]
            error_table = pd.concat([error_table, check_if_list(vaccine_data, curr_col, list_values)])
        if (curr_col in ['SARS-CoV-2_Vaccination_Date_Duration_From_Visit1']):
            error_table = pd.concat([error_table,check_is_number(vaccine_data, curr_col, -1e9, 1e9, NA_Allowed=True)])
    return error_table

def check_id_field(data_table, re, field_name, pattern_str, CBC_ID, pattern_error):
    wrong_cbc_id = data_table[data_table[field_name].apply(lambda x: x[:2] not in [str(CBC_ID)])]
    invalid_id = data_table[data_table[field_name].apply(lambda x: re.compile('^' + str(CBC_ID) + pattern_str).match(str(x)) is None)]

    wrong_cbc_id = add_df_cols(wrong_cbc_id, field_name, "CBC code found is wrong. Expecting CBC Code (" + str(CBC_ID) + ")")
    invalid_id = add_df_cols(invalid_id, field_name,  "ID is Not Valid Format, Expecting " + pattern_error)

    error_table = pd.concat([wrong_cbc_id, invalid_id])
    error_table = error_table.rename(columns={field_name: "Column_Value"})
    return error_table


def check_is_number(data_table, curr_col, min_val, max_val, **kwargs):
    try:
        Not_a_number = data_table[data_table[curr_col].apply(lambda x: isinstance(x, (int, float)) is False)]
        numeric_data = data_table[data_table[curr_col].apply(lambda x: isinstance(x, (int, float)) is True)]
        out_of_range = numeric_data.query("`{0}` > @max_val or `{0}` < @min_val".format(curr_col))
    except Exception as e:
        print(e)

    if "NA_Allowed" in kwargs:
        if kwargs["NA_Allowed"] is True:
            Not_a_number = Not_a_number.query(f"`{curr_col}` != 'N/A'")

    Not_a_number = add_df_cols(Not_a_number, curr_col, "Value is not a numeric value")
    out_of_range = add_df_cols(out_of_range, curr_col, f"Value must be a number between {min_val} and {max_val}")

    error_table = pd.concat([Not_a_number, out_of_range])
    error_table = error_table.rename(columns={curr_col: "Column_Value"})
    return error_table


def check_if_list(data_table, curr_col, list_values):
    x = [i for i in data_table.index if data_table[curr_col][i] not in list_values]
    error_data = data_table.loc[x]
    error_data = add_df_cols(error_data, curr_col, f"Value is not an acceptable term, should be one the following: {list_values}")
    error_data = error_data.rename(columns={curr_col: "Column_Value"})
    return error_data


def check_if_date(data_table, curr_col):
    error_table = pd.DataFrame(columns=["Column_Name", "Column_Value", "Error_Message"])
    for curr_index in data_table.index:
        try:
            curr_date = data_table[curr_col][curr_index]
            try:
                future_logic = curr_date.date() > datetime.date.today()  #date in the future
            except Exception:
                 future_logic = curr_date > datetime.date.today()  #date in the future
            weekday_logic = curr_date.strftime('%A') != "Sunday"  #date not a sunday
            if future_logic is True and weekday_logic is False:
                error_msg = "Value is a Sunday but exists in the future"
            if future_logic is False and weekday_logic is True:
                error_msg = "Value is a valid date but is not a Sunday"
            if future_logic is True and weekday_logic is True:
                error_msg = "Value is a future date and is also not a Sunday"
            else:
                continue
            error_table.loc[len(error_table)] = [curr_col, data_table[curr_col][curr_index], error_msg]
        except Exception:
            error_table.loc[len(error_table)] = [curr_col, data_table[curr_col][curr_index], "Value is not a parsable date"]
    return error_table


def display_error_line(ex):
    trace = []
    tb = ex.__traceback__
    while tb is not None:
        trace.append({"filename": tb.tb_frame.f_code.co_filename,
                      "name": tb.tb_frame.f_code.co_name,
                      "lineno": tb.tb_lineno})
        tb = tb.tb_next
    print(str({'type': type(ex).__name__, 'message': str(ex), 'trace': trace}))


def connect_to_sql_db(sd, file_dbname):
    print(f"\n##   Attempting to Connect to {file_dbname}   ##")
    host_client = "seronet-dev-instance.cwe7vdnqsvxr.us-east-1.rds.amazonaws.com"
    user_name = "seronet-datauser4"
    user_password = "1ebe65925b6bc578f93a43ccdb2ff972"  # non-prod

    creds = {'usr': user_name, 'pwd': user_password, 'hst': host_client, "prt": 3306, 'dbn': file_dbname}
    connstr = "mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}"
    engine = sd.create_engine(connstr.format(**creds))
    conn = engine.connect()
    return engine, conn


def upload_data(data_table, table_name, engine, conn, primary_col):
    sql_df = pd.read_sql(f"Select * FROM {table_name}", conn)
    sql_df.fillna("N/A", inplace=True)
    
    for curr_col in sql_df:
        sql_df[curr_col] = [convert_data_type(c) for c in sql_df[curr_col]]

    for curr_col in sql_df.columns:
        if curr_col in ['Site_Cohort_Name', 'Primary_Cohort', 'Serum_Volume_For_FNL', 'Num_PBMC_Vials_For_FNL', 'PBMC_Concentration']:  #numeric but N/A allowed
            sql_df[curr_col] = [str(c) for c in sql_df[curr_col]]
            
    for curr_col in data_table.columns:
        if curr_col in ['Site_Cohort_Name']:
            data_table[curr_col] = [str(c) for c in data_table[curr_col]]
        else:
            data_table[curr_col] = [convert_data_type(c) for c in data_table[curr_col]]
        if curr_col in ['Site_Cohort_Name', 'Primary_Cohort', 'Serum_Volume_For_FNL', 'Num_PBMC_Vials_For_FNL', 'PBMC_Concentration']:  #numeric but N/A allowed
            data_table[curr_col] = [str(c) for c in data_table[curr_col]]
    if "Sunday_Prior_To_Visit_1" in data_table.columns:
        data_table["Sunday_Prior_To_Visit_1"] = [i.date() if isinstance(i, datetime.datetime) else i for i in data_table["Sunday_Prior_To_Visit_1"]]

    if "Visit_Date_Duration_From_Visit_1" in data_table.columns:
        data_table["Visit_Date_Duration_From_Visit_1"] = data_table["Visit_Date_Duration_From_Visit_1"].replace("\.0", "", regex=True)
        
    if "PBMC_Concentration" in sql_df.columns and "Num_PBMC_Vials_For_FNL" in sql_df.columns:
        sql_df["PBMC_Concentration"] = sql_df["PBMC_Concentration"].replace("\.0", "", regex=True)
        sql_df["Num_PBMC_Vials_For_FNL"] = sql_df["Num_PBMC_Vials_For_FNL"].replace("\.0", "", regex=True)
    try:
        check_data = data_table.merge(sql_df, how="left", indicator="first_pass")
        check_data = check_data.query("first_pass == 'left_only'")
        primary_keys = ["Research_Participant_ID"] + primary_col
    except Exception as e:
        print(e)

    try:
        check_data = check_data.merge(sql_df[primary_keys], how="left", on=primary_keys, indicator="second_pass")
        new_data = check_data.query("second_pass == 'left_only'")   # primary keys do not exist
        update_data = check_data.query("second_pass == 'both'")     # primary keys exist but data update
    except Exception as e:
        print(e)

    new_data.drop(["first_pass", "second_pass"], axis=1, inplace=True)
    update_data.drop(["first_pass", "second_pass"], axis=1, inplace=True)

    try:
        if len(new_data) > 0:
            new_data.to_sql(name=table_name, con=engine, if_exists="append", index=False)
            conn.connection.commit()
    except Exception as e:
        print(e)

    if len(update_data) > 0:
        update_tables(conn, engine, primary_keys, update_data, table_name)


def update_tables(conn, engine, primary_keys, update_table, sql_table):
    key_str = ['`' + str(s) + '`' + " like '%s'" for s in primary_keys]
    key_str = " and ".join(key_str)

    col_list = update_table.columns.tolist()
    col_list = [i for i in col_list if i not in primary_keys]

    print(f"there are {len(update_table)} records to be udpated for table {sql_table}")
    for index in update_table.index:
        try:
            curr_data = update_table.loc[index, col_list].values.tolist()
            primary_value = update_table.loc[index, primary_keys].values.tolist()

            update_str = ["`" + i + "` = '" + str(j) + "'" for i, j in zip(col_list, curr_data)]
            update_str = ', '.join(update_str)
            #update_str = update_str.replace("'-1e9'", "NULL")
            #update_str = update_str.replace("'N/A'", "NULL")

            sql_query = (f"UPDATE {sql_table} set {update_str} where {key_str %tuple(primary_value)}")
            engine.execute(sql_query)
        except Exception as e:
            print(e)
        finally:
            conn.connection.commit()


validate_accrual()