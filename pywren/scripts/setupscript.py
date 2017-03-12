import pywrencli
import click
import os
import pywren.wrenconfig
def click_validate_prompt(message, default, validate_func, fail_msg =""):
    """
    Click wrapper that repeats prompt until acceptable answer
    """
    while True:
        res = click.prompt(message, default)
        if validate_func(res):
            return res
        else:
            if fail_msg != "":
                click.echo(fail_msg.format(res))

def check_aws_region_valid(aws_region_str):
    if aws_region_str in ['us-west-2']:
        return True
    return False

def check_overwrite_function(filename):
    if os.path.exists(filename):
        return click.confirm("{} already exists, would you like to overwrite?".format(filename))
    return True


@click.command()
@click.pass_context
def interactive_setup(ctx):

    click.echo("This is the pywren interactive setup script")
    try:
        #first we will try and make sure AWS is set up

        account_id = ctx.invoke(pywrencli.get_aws_account_id, False)
        click.echo("Your AWS configuration appears to be set up, and your account ID is {}".format(account_id))
    except Exception as e:
        raise

    click.echo("This interactive script will set up your initial pywren configuration. The defaults are generally fine if this is your first time using pywren.")
    
    # first, what is your default AWS region? 
    aws_region = click_validate_prompt("What is your default aws region?", 
                                 default="us-west-2", 
                                 validate_func = check_aws_region_valid, 
                                 fail_msg = "{} not a valid aws region")
    # FIXME make sure this is a valid region
    
    
    # if config file exists, ask before overwriting
    config_filename = click_validate_prompt("Location for config file: ", 
                                            default=pywren.wrenconfig.get_default_home_filename(), 
                                            validate_func=check_overwrite_function)
    
    
    
    def check_bucket_exists(s3bucket):
        # if bucket exists

        # if region = 
        # return true

        click.confirm("Bucket does not currently exist, would you like to create it?")

        # create the bucket


    s3_bucket = click_validate_python("pywren requires an s3 bucket to store intermediate data. What s3 bucket would you like to use?", 
                                      default="pywren.storage", check_bucket_exists)
    
    
    click.echo("Pywren prefixes every object it puts in S3 with a particular prefix")
    pywren_prefix = click_validate_python("pywren s3 prefix: ", default="pywren.jobs")

    click.echo("This script does not support advanced configuration of pywren lamdab settings, to do that you must manually edit the command line")

    click.echo("pywren standalone mode uses dedicated AWS instances to run pywren tasks. This is more flexible, but more expensive with fewer simultaneous workers."
    use_standalone = click.confirm("Would you like to enable pywren standalone mode?")

    ctx.invoke(pywrencli.create_config, 
               filename=config_filename, 
               aws_region = aws_region, 

               bucket_name
    # # Then we will create the default config file
    # ctx.invoke(create_config)

    # # Create the bucket
    # ctx.invoke(create_bucket)

    # # then we will create the role
    # ctx.invoke(create_role)

    # # then we deploy the lambda
    # ctx.invoke(deploy_lambda)

    # # then we ask if they want stand-alone mode? 

    # # Then we create the queue

    # # then we create the instance profile


if __name__ == '__main__':
    interactive_setup()
