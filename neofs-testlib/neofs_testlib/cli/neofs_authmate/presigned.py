from neofs_testlib.cli.cli_command import CliCommand
from neofs_testlib.shell import CommandResult


class NeofsAuthmatePresigned(CliCommand):
    def generate_presigned_url(
        self,
        endpoint: str,
        method: str,
        bucket: str,
        object: str,
        lifetime: str,
        aws_secret_access_key: str,
        aws_access_key_id: str,
    ) -> CommandResult:
        """Generate presigned URL

        Args:
            endpoint: Endpoint of s3-gw
            method: HTTP method to perform action
            bucket: Bucket name to perform action
            object: Object name to perform action
            lifetime: Lifetime of presigned URL
            aws-access-key-id: AWS access key id to sign the URL
            aws-secret-access-key: AWS access secret access key to sign the URL

        Returns:
            Command's result.
        """
        return self._execute(
            "generate-presigned-url",
            **{param: param_value for param, param_value in locals().items() if param not in ["self"]},
        )
