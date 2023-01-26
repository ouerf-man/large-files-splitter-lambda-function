# Large CSV files splitter lambda function

This nodejs script serves as a lambda function that get's executed when a csv file is uploaded to S3,

This function splits this large csv file, zip the output files and upload them to another S3.