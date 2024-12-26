import boto3


class RedshiftExecutor:
    def __init__(self, redshift_creds=None, redshift_client=None):

        self.redshift_creds=redshift_creds 
        if redshift_client is not None:
            self.redshift_client = redshift_client
        else:
            self.redshift_client = boto3.client("redshift-data")

    def check_status(self, response_id):
        self.status = "Not checked"
        self.status_messages = "Not checked"

        response = self.redshift_client.describe_statement(Id=response_id)
        while response['Status'] != 'FINISHED':
            response = self.redshift_client.describe_statement(Id=response_id)

            self.status = response["Status"]
            if response['Status'] == 'ABORTED':
                self.status_message = "Query was Canceled"
                break
            elif response['Status'] == 'FAILED':
                # Extracting the detailed error message
                error_details = response.get('Error', 'No error message provided by Redshift')
                self.status_message = error_details
                break
            elif response['Status'] == 'FINISHED':
                self.status_message = "Statement finished sucsessfully"
                break

    def execute_statement(self, query_string="", jobname="unnamed"):
        self.response_id = "new query"

        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.redshift_creds["ClusterIdentifier"],
            Database=self.redshift_creds["Database"],
            DbUser=self.redshift_creds["DbUser"],
            Sql=query_string,
            StatementName=jobname,
        )
        self.execution_status = response['ResponseMetadata']["HTTPStatusCode"]
        self.response_id=response["Id"]

        self.check_status(self.response_id)
