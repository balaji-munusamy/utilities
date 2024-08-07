import boto3

class AWSUtils():

    @staticmethod
    def get_parameter(param_name: str, region_name: str) -> str:
        """
        Read the value of a parameter from AWS Parameter Store.

        Parameters:
        - param_name (str): Name of the parameter.

        Returns:
        - param_value (str): Value of the parameter.
        """
        ssm_client = boto3.client('ssm', region_name=region_name)
        response = ssm_client.get_parameter(Name=param_name)#, WithDecryption=True)
        parameter_value = response['Parameter']['Value']
        return parameter_value