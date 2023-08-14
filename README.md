# CLOUD A2
## HOW TO RUN
- missing_info.py can be run using `python3 missing_info.py`
- Please place AWS config information into a file called `config.conf` in the format of: 
>[default]
aws_access_key_id = ACCESS_KEY_ID
aws_secret_access_key = ACCESS_KEY

## Notes about Dynamo Functions
- In order to perform Dynamo Functions such as adding a record (add_record()), updating a record (update_record()), and deleting a record (delete_record()), you need the Partition and Sort Key.
- Population and Country Data tables use Country as Partition key and ISO3 as Sort key.
- Economic Data table uses Country as Partition key and Currency as Sort key.

## NORMAL BEHAVIOURS
- If it looks like nothing is happening (especially when running scripts for the first time), it's likely due to tables being created/deleted. Each table deletion/creation takes 5-10 seconds.

## SIDE NOTES
- This was mainly tested on Python 3.9.6, so if possible, please run it at that version.