import time
import boto3
import hashlib

from botocore.exceptions import ClientError


def _object_to_dict(obj):
    if hasattr(obj, "__dict__"):  # Check if it's an object with __dict__
        return {key: _object_to_dict(value) for key, value in vars(obj).items()}
    elif isinstance(obj, list):  # Handle lists recursively
        return [_object_to_dict(item) for item in obj]
    elif isinstance(obj, dict):  # Handle dictionaries recursively
        return {key: _object_to_dict(value) for key, value in obj.items()}
    else:  # Return the value as is if it's not an object, list, or dictionary
        return obj


class _cloudformation:
    """
    CloudFormation helper class

    Attributes:
        in_progress_statuses (list): List of in progress statuses
        successful_statuses (list): List of successful statuses
        failed_statuses (list): List of failed statuses
        rollback_statuses (list): List of rollback statuses
        special_cases (list): List of special cases
    """

    in_progress_statuses = [
        "CREATE_IN_PROGRESS",
        "ROLLBACK_IN_PROGRESS",
        "DELETE_IN_PROGRESS",
        "UPDATE_IN_PROGRESS",
        "UPDATE_ROLLBACK_IN_PROGRESS",
        "REVIEW_IN_PROGRESS",
        "IMPORT_IN_PROGRESS",
        "IMPORT_ROLLBACK_IN_PROGRESS",
        "DELETE_IN_PROGRESS",
        "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
    ]

    completed_statuses = [
        "CREATE_COMPLETE",
        "DELETE_COMPLETE",
        "UPDATE_COMPLETE",
        "UPDATE_ROLLBACK_COMPLETE",
        "IMPORT_COMPLETE",
        "IMPORT_ROLLBACK_COMPLETE",
        "DELETE_COMPLETE",
        "ROLLBACK_COMPLETE",
        "UPDATE_ROLLBACK_COMPLETE",
        "IMPORT_ROLLBACK_COMPLETE",
    ]
    failed_statuses = [
        "CREATE_FAILED",
        "ROLLBACK_FAILED",
        "DELETE_FAILED",
        "UPDATE_FAILED",
        "UPDATE_ROLLBACK_FAILED",
        "IMPORT_FAILED",
        "IMPORT_ROLLBACK_FAILED",
        "DELETE_FAILED",
        "ROLLBACK_COMPLETE",
    ]
    rollback_statuses = [
        "ROLLBACK_COMPLETE",
        "UPDATE_ROLLBACK_COMPLETE",
        "IMPORT_ROLLBACK_COMPLETE",
    ]
    special_cases = [
        "DELETE_IN_PROGRESS",
        "DELETE_COMPLETE",
        "DELETE_FAILED",
    ]

    def deploy(self, user, prefix):
        """
        Deploy the CloudFormation stack

        Args:
            user (dict): builder result with the following mandatory keys:
                aws_profile (str): AWS profile name that will be used for deployment
                aws_stack (str): AWS stack name
                aws_region (str): AWS region
                aws_bucket (str): Name of the S3 bucket where the CloudFormation template is stored
                aws_stack_hash (str): AWS stack hash
                timestamp (int): EPOCH timestamp when the build started
            prefix (str): Prefix of the S3 bucket path where the deployment files are stored

        Raises:
            ValueError: If the stack is in progress

        Returns:
            None
        """

        # Create the AWS session
        self._aws_session = boto3.session.Session(
            profile_name=user.config["aws_profile"]
        )

        # Create the CloudFormation client
        self._cloudformation_client = self._aws_session.client(
            "cloudformation", region_name=user.config["aws_region"]
        )

        # Check the aws_stack status
        aws_stack_status = self.check_stack(user.config["aws_stack"])
        print(f"Stack status: {aws_stack_status}")

        # Handle the aws_stack
        if aws_stack_status == "DOES_NOT_EXIST":
            print("Creating aws_stack")
            self._cloudformation_client.create_stack(
        elif aws_stack_status in self.completed_statuses:
            try:
                print("Updating aws_stack")
                self._cloudformation_client.update_stack(
                    StackName=user.config["aws_stack"],
                    TemplateURL=f"https://{user.config["aws_bucket"]}.s3.amazonaws.com/{prefix}/{user.config["timestamp"]}-{user.config["aws_stack_hash"]}.json",
                    Capabilities=["CAPABILITY_NAMED_IAM"],
                )
            except ClientError as e:
                if "No updates are to be performed" in str(e):
                    print("No updates detected. Skipping stack update.")
                else:
                    raise
        elif (
            aws_stack_status in self.failed_statuses
            or aws_stack_status in self.rollback_statuses
        ):
            print("Handling failed aws_stack")
            self._cloudformation_client.delete_stack(StackName=user.config["aws_stack"])
            self.deploy_wait(user)
            if self.check_stack(user.config["aws_stack"]) != "DOES_NOT_EXIST":
                print("Failed to delete stack, cannot continue")
                raise ValueError("Failed to delete stack, cannot continue")
            print("Creating aws_stack")
            self._cloudformation_client.create_stack(
                StackName=user.config["aws_stack"],
                TemplateURL=f"https://{user.config["aws_bucket"]}.s3.amazonaws.com/{prefix}/{user.config["timestamp"]}-{user.config["aws_stack_hash"]}.json",
                Capabilities=["CAPABILITY_NAMED_IAM"],
            )
        elif aws_stack_status in self.in_progress_statuses:
            raise ValueError("Stack is in progress")

        # Close the CloudFormation client
        self._cloudformation_client.close()

        del self._aws_session

    def deploy_wait(self, user, timeout=600):

        # Create session
        self._aws_session = boto3.session.Session(
            profile_name=user.config["aws_profile"]
        )

        # Create client
        self._cloudformation_client = self._aws_session.client(
            "cloudformation", region_name=user.config["aws_region"]
        )

        while True:

            # Checking that the stack exists
            status = self.check_stack(user.config["aws_stack"])
            if status == "DOES_NOT_EXIST":
                print("The stack does not exist")
                break

            # Iterating while ongoing or timeout
            start_time = time.time()
            while (
                status in self.in_progress_statuses and time.time() - start_time < timeout
            ):
                time.sleep(10)
                status = self.check_stack(user.config["aws_stack"])

            # Reporting the status
            if status in self.completed_statuses or status == "DOES_NOT_EXIST":
                print("Stack procedure successful")
            elif status in self.failed_statuses:
                print("Stack procedure failed")
            else:
                print("Stack procedure timed out")
            print(f"Stack status: {status}")
            break

        # Closing the client
        self._cloudformation_client.close()

        # Deleting the session
        del self._aws_session

    def check_stack(self, name):
        """
        Check the status of the CloudFormation stack

        Args:
            name (str): Name of the CloudFormation stack

        Returns:
            str: Status of the CloudFormation stack or "DOES_NOT_EXIST" if the stack does not exist
        """

        try:
            response = self._cloudformation_client.describe_stacks(StackName=name)
            return response.get("Stacks")[0].get("StackStatus")
        except ClientError as e:
            if "does not exist" in str(e):
                return "DOES_NOT_EXIST"
            else:
                raise


class _aws:
    """
    AWS helper class

    Attributes:
        cloudformation (cloudformation): CloudFormation helper class
    """

    cloudformation = _cloudformation()


class commons:
    """
    Tlaloc commons helper class

    Attributes:
        http_methods (list): List of HTTP methods
        aws(_aws): AWS helper class

    Returns:
        None
    """

    http_methods = [
        "GET",
        "POST",
        "PUT",
        "DELETE",
        "PATCH",
        "HEAD",
        "OPTIONS",
        "ANY",
    ]

    def get_hash(string):
        """
        Get the MD5 hash of a string

        Args:
            string (str): String to be hashed

        Returns:
            str: MD5 hash of the string
        """
        return hashlib.md5(string.encode("utf-8")).hexdigest()

    aws = _aws()
