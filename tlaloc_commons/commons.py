import time
import json
import boto3
import hashlib

from botocore.exceptions import ClientError


def _object_to_dict(obj, level=0):
    if level > 10:
        return obj
    elif hasattr(obj, "__dict__"):
        return {
            key: _object_to_dict(value, level + 1) for key, value in vars(obj).items()
        }
    elif isinstance(obj, list):
        return [_object_to_dict(item, level + 1) for item in obj]
    elif isinstance(obj, dict):
        return {key: _object_to_dict(value, level + 1) for key, value in obj.items()}
    else:
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
        "DELETE_COMPLETE",
        "ROLLBACK_COMPLETE",
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
    update_impossible_statuses = [
        "ROLLBACK_COMPLETE",
        "ROLLBACK_FAILED",
        "DELETE_FAILED",
        "UPDATE_ROLLBACK_FAILED",
        "IMPORT_ROLLBACK_FAILED",
        "DELETE_FAILED",
    ]

    def deploy(self, config, capabilities=[], parameters=[], tags=[]):
        """
        Deploy the CloudFormation stack

        Args:
            user (dict): builder result with the following mandatory keys:
                aws_profile (str): AWS profile name that will be used for deployment
                aws_stack (str): AWS stack name
                aws_region (str): AWS region
                aws_bucket (str): Name of the S3 bucket where the CloudFormation template is stored
                aws_folder (str): Folder in the S3 bucket where the deployment files are stored
                aws_template_file (str): AWS stack hash
                aws_template_body (str): Body of the CloudFormation template

        Raises:
            ValueError: If the stack is in progress

        Returns:
            None
        """
        # Transforming class to dictionary
        config = _object_to_dict(config)

        # Create the AWS session
        self._aws_session = boto3.session.Session(
            profile_name=config["config"]["aws_profile"]
        )

        # Create the CloudFormation client
        self._cloudformation_client = self._aws_session.client(
            "cloudformation", region_name=config["config"]["aws_region"]
        )

        # Check the aws_stack status
        aws_stack_status = self.check_stack(config["config"]["aws_stack"])
        print(f"Stack status: {aws_stack_status}")

        if "aws_template_file" in config["config"]:
            if aws_stack_status == "DOES_NOT_EXIST":
                print("Creating aws_stack")
                self._cloudformation_client.create_stack(
                    StackName=config["config"]["aws_stack"],
                    TemplateURL=f"https://{config["config"]["aws_bucket"]}.s3.amazonaws.com/{config["config"]["aws_folder"]}/{config["config"]["aws_template_file"]}",
                    Capabilities=capabilities,
                    Parameters=parameters,
                    Tags=tags,
                )
            elif aws_stack_status in self.in_progress_statuses:
                raise ValueError("Stack is in progress")
            elif (
                aws_stack_status in self.failed_statuses
                or aws_stack_status in self.update_impossible_statuses
            ):
                print("Handling failed aws_stack")
                self._cloudformation_client.delete_stack(
                    StackName=config["config"]["aws_stack"]
                )
                self.deploy_wait(config)
                if self.check_stack(config["config"]["aws_stack"]) != "DOES_NOT_EXIST":
                    print("Failed to delete stack, cannot continue")
                    raise ValueError("Failed to delete stack, cannot continue")
                print("Creating aws_stack")
                self._cloudformation_client.create_stack(
                    StackName=config["config"]["aws_stack"],
                    TemplateURL=f"https://{config["config"]["aws_bucket"]}.s3.amazonaws.com/{config["config"]["aws_folder"]}/{config["config"]["aws_template_file"]}",
                    Capabilities=capabilities,
                    Parameters=parameters,
                    Tags=tags,
                )
            elif aws_stack_status in self.completed_statuses:
                try:
                    print("Creating change set")
                    change_set = self._cloudformation_client.create_change_set(
                        StackName=config["config"]["aws_stack"],
                        TemplateURL=f"https://{config["config"]["aws_bucket"]}.s3.amazonaws.com/{config["config"]["aws_folder"]}/{config["config"]["aws_template_file"]}",
                        Capabilities=capabilities,
                        Parameters=parameters,
                        Tags=tags,
                        ChangeSetType="UPDATE",
                        ChangeSetName=f"ChangeSet{config['config']['timestamp']}",
                    )
                    change_set_description = (
                        self._cloudformation_client.describe_change_set(
                            StackName=config["config"]["aws_stack"],
                            ChangeSetName=f"ChangeSet{config['config']['timestamp']}",
                        )
                    )
                    if change_set_description["Status"] == "FAILED":
                        if (
                            change_set_description["StatusReason"]
                            == "The submitted information didn't contain changes. Submit different information to create a change set."
                        ):
                            print("No updates detected. Skipping stack update.")
                            self._cloudformation_client.delete_change_set(
                                StackName=config["config"]["aws_stack"],
                                ChangeSetName=f"ChangeSet{config['config']['timestamp']}",
                            )
                            return
                        else:
                            raise ValueError(
                                f"Failed to create change set: {change_set_description['StatusReason']}"
                            )
                    print("Waiting for change set to be created")
                    waiter = self._cloudformation_client.get_waiter(
                        "change_set_create_complete"
                    )
                    waiter.wait(
                        ChangeSetName=change_set["Id"],
                        WaiterConfig={
                            "Delay": 10,
                            "MaxAttempts": 30,
                        },
                    )
                    print("Executing change set")
                    self._cloudformation_client.execute_change_set(
                        StackName=config["config"]["aws_stack"],
                        ChangeSetName=f"ChangeSet{config['config']['timestamp']}",
                    )
                    print("Waiting for stack to be updated")
                    waiter = self._cloudformation_client.get_waiter(
                        "stack_update_complete"
                    )
                    waiter.wait(
                        StackName=config["config"]["aws_stack"],
                        WaiterConfig={
                            "Delay": 10,
                            "MaxAttempts": 30,
                        },
                    )
                    stack_status = self.check_stack(config["config"]["aws_stack"])
                    print(f"Stack status: {stack_status}")
                except Exception as e:
                    if "No updates are to be performed" in str(e):
                        print("No updates detected. Skipping stack update.")
                    else:
                        raise
        elif "aws_template_body" in config["config"]:
            if aws_stack_status == "DOES_NOT_EXIST":
                print("Creating aws_stack")
                self._cloudformation_client.create_stack(
                    StackName=config["config"]["aws_stack"],
                    TemplateURL=f"https://{config["config"]["aws_bucket"]}.s3.amazonaws.com/{config["config"]["aws_folder"]}/{config["config"]["aws_template_file"]}",
                    Capabilities=capabilities,
                    Parameters=parameters,
                    Tags=tags,
                )
            elif aws_stack_status in self.in_progress_statuses:
                raise ValueError("Stack is in progress")
            elif aws_stack_status in self.failed_statuses:
                print("Handling failed aws_stack")
                self._cloudformation_client.delete_stack(
                    StackName=config["config"]["aws_stack"]
                )
                self.deploy_wait(config)
                if self.check_stack(config["config"]["aws_stack"]) != "DOES_NOT_EXIST":
                    print("Failed to delete stack, cannot continue")
                    raise ValueError("Failed to delete stack, cannot continue")
                print("Creating aws_stack")
                self._cloudformation_client.create_stack(
                    StackName=config["config"]["aws_stack"],
                    TemplateURL=f"https://{config["config"]["aws_bucket"]}.s3.amazonaws.com/{config["config"]["aws_folder"]}/{config["config"]["aws_template_file"]}",
                    Capabilities=capabilities,
                    Parameters=parameters,
                    Tags=tags,
                )
            elif aws_stack_status in self.completed_statuses:
                try:
                    print("Creating change set")
                    change_set = self._cloudformation_client.create_change_set(
                        StackName=config["config"]["aws_stack"],
                        TemplateBody=config["config"]["aws_template_body"],
                        Capabilities=capabilities,
                        Parameters=parameters,
                        Tags=tags,
                        ChangeSetType="UPDATE",
                        ChangeSetName=f"{config['config']['timestamp']}-change-set",
                    )
                    print("Waiting for change set to be created")
                    waiter = self._cloudformation_client.get_waiter(
                        "change_set_create_complete"
                    )
                    waiter.wait(
                        ChangeSetName=change_set["Id"],
                        WaiterConfig={
                            "Delay": 10,
                            "MaxAttempts": 30,
                        },
                    )
                    print("Executing change set")
                    self._cloudformation_client.execute_change_set(
                        ChangeSetName=f"{config['config']['timestamp']}-change-set",
                    )
                    print("Waiting for stack to be updated")
                    waiter = self._cloudformation_client.get_waiter(
                        "stack_update_complete"
                    )
                    waiter.wait(
                        StackName=config["config"]["aws_stack"],
                        WaiterConfig={
                            "Delay": 10,
                            "MaxAttempts": 30,
                        },
                    )
                    stack_status = self.check_stack(config["config"]["aws_stack"])
                    print(f"Stack status: {stack_status}")
                except Exception as e:
                    if "No updates are to be performed" in str(e):
                        print("No updates detected. Skipping stack update.")
                    else:
                        raise
        else:
            raise ValueError("No template provided")

        # Close the CloudFormation client
        self._cloudformation_client.close()

        del self._aws_session

    def get_output(self, user, output_name):
        """
        Get the outputs of the CloudFormation stack

        Args:
            name (str): Name of the CloudFormation stack

        Returns:
            dict: Outputs of the CloudFormation stack
        """

        # Create the AWS session
        self._aws_session = boto3.session.Session(
            profile_name=user["config"]["aws_profile"]
        )

        # Create the CloudFormation client
        self._cloudformation_client = self._aws_session.client(
            "cloudformation", region_name=user["config"]["aws_region"]
        )

        # Looking up the stack
        value = None
        status = "DOES_NOT_EXIST"
        while True:

            # Check the stack status
            status = self.check_stack(user["config"]["aws_stack"])
            print(f"Stack status: {status}")
            if (
                status == "DOES_NOT_EXIST"
                or status not in self.completed_statuses
                or status in self.failed_statuses
            ):
                break

            # Retrieve the stack outputs
            response = self._cloudformation_client.describe_stacks(
                StackName=user["config"]["aws_stack"]
            )
            outputs = response.get("Stacks")[0].get("Outputs")

            # Find the value of the output
            while True:
                for output in outputs:
                    if output["OutputKey"] == output_name:
                        value = output["OutputValue"]
                        break
                break
            break

        # Close the CloudFormation client
        self._cloudformation_client.close()

        # Delete the session
        del self._aws_session

        if (
            status == "DOES_NOT_EXIST"
            or status in self.failed_statuses
            or status not in self.completed_statuses
        ):
            raise ValueError(f"Stack is not in a valid state: {status}")

        if value is None:
            raise ValueError(f"Output {output_name} not found")

        return value

    def deploy_wait(self, user, timeout=600):

        # Transforming class to dictionary
        user = _object_to_dict(user)

        # Create session
        self._aws_session_wait = boto3.session.Session(
            profile_name=user["config"]["aws_profile"]
        )

        # Create client
        self._cloudformation_client = self._aws_session_wait.client(
            "cloudformation", region_name=user["config"]["aws_region"]
        )

        while True:

            # Checking that the stack exists
            status = self.check_stack(user["config"]["aws_stack"])
            if status == "DOES_NOT_EXIST":
                print("The stack does not exist")
                break

            # Iterating while ongoing or timeout
            start_time = time.time()
            while (
                status in self.in_progress_statuses
                and time.time() - start_time < timeout
            ):
                time.sleep(10)
                status = self.check_stack(user["config"]["aws_stack"])

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
        del self._aws_session_wait

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


class _ssm:
    """
    SSM helper class

    Attributes:
        ssm (ssm): SSM helper class
    """

    def get_parameter(self, name):
        """
        Get the value of an SSM parameter

        Args:
            name (str): Name of the SSM parameter

        Returns:
            str: Value of the SSM parameter
        """

        # Create the SSM client
        self._ssm_client = boto3.client("ssm")

        # Get the parameter
        response = self._ssm_client.get_parameter(Name=name)

        # Close the SSM client
        self._ssm_client.close()

        return response["Parameter"]["Value"]


class _aws:
    """
    AWS helper class

    Attributes:
        cloudformation (cloudformation): CloudFormation helper class
        regions (list): List of valid AWS regions
    """

    cloudformation = _cloudformation()

    ssm = _ssm()

    regions = [
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "af-south-1",
        "ap-east-1",
        "ap-south-1",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-northeast-3",
        "ap-southeast-1",
        "ap-southeast-2",
        "ca-central-1",
        "eu-central-1",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "eu-north-1",
        "eu-south-1",
        "me-south-1",
        "sa-east-1",
    ]


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
