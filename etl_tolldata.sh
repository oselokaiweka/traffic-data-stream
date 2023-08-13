#!/bin/sh

# This shell script extracts and consolidates four differently formatted files 
# located within an archived file and stored in a cloud storage. The four zipped 
# files contain several highway toll traffic records that needs to be loaded into 
# a database for decongestion strategy analysis.

# Creating staging directories
staging=$AIRFLOW_HOME/tolldata-etl/staging
staging2=$AIRFLOW_HOME/tolldata-etl/staging/staging2
if [ ! -d $staging ]; then mkdir $staging; fi
if [ ! -d $staging2 ]; then mkdir $staging2; fi
 
web_source=https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz
source_zip=$AIRFLOW_HOME/tolldata-etl/tolldata.tgz
if [ ! -f $source_zip ]; 
    then wget -P $AIRFLOW_HOME/tolldata-etl $web_source
    echo "archive downloaded, unzipping files..."
else
    echo "unzipping files from archive..."

# Unzip downloaded data
if tar -xzf $AIRFLOW_HOME/tolldata-etl/tolldata.tgz -C $staging2; then
    echo "Unzip file complete"

    # Extract data from vehicle-data.csv
    if cut -d ',' -f 1-4 $staging2/vehicle-data.csv > $staging2/csv_data.csv; then
        echo "Extract fieslds 1 to 4 from vehicle-data.csv, save as csv_data.csv completed"

        # Extract data from tollplaza-data.tsv
        if cut -f 5-7 $staging2/tollplaza-data.tsv | tr -d '\r' | tr '\t' ',' > $staging2/tsv_data.csv; then
            echo "Extract fields 5 to 7 from tollplaza-data.tsv, save as tsv_data.csv completed"

            # Extract data from payment-data.txt. Inconsisstencies in leading fields, will adopt awk for extraction
            if awk '{field1=substr($0, length($0)-8, 3); field2=substr($0, length($0)-4); print field1, field2}' $staging2/payment-data.txt | tr -s ' ' ',' > $staging2/fixed_width_data.csv; then
                echo "Extract field 6 & 7 from payment-data.txt, save as fixed_width_data.csv completed"

                # Merge fields from all three files
                if paste -d',' $staging2/csv_data.csv $staging2/tsv_data.csv $staging2/fixed_width_data.csv > $staging2/extracted_data.csv; then
                    echo "Merge fields from all files, save as extracted_data.csv completed"

                    # Capitalize the vehicle type field
                    if awk 'BEGIN{FS=OFS=","} { $4 = toupper($4) }1' $staging2/extracted_data.csv > $staging/transformed_data.csv; then
                        echo "Capitalize field 4 of extracted_data.csv, save as transformed_data.csv completed"

                    else echo "Vehicle type capitalization error, process terminated"; rm -rf $staging
                        exit 1; fi
                else echo "Merge file error, process terminated"; rm -rf $staging
                    exit 1; fi
            else echo "Payment data extraction error, process terminated"; rm -rf $staging
                exit 1; fi
        else echo "Tollplaza data extraction error, process terminated"; rm -rf $staging
            exit 1; fi
    else echo "Vehicle data extraction error, process terminated"; rm -rf $staging
        exit 1; fi
else echo "Could not unzip file, process terminated"; rm -rf $staging
    exit 1; fi

# Cleaning up temporary files and staging directory
echo "Removing temp files, staging dir..."
if rm -rf $staging; 
    then echo "Temp files, dir removed\n"
else echo "Could not remove files, staging dir"; 
fi

# ETL COMPLETED WITHOUT ERRORS
echo "* * * PROCESS COMPLETE * * *"

