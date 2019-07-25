#!/bin/bash
# vim: ts=4 sw=4 et

# Welcome to the Biohub seqbot download SLURM wrapper!
# You will want to use this if your Biohub contact gave you a script to run,
# to download an Illumina runfolder, but you want to download it on Sherlock.

# This script will tell the AWS command to write files to whatever directory
# you are in at the time you submitted this job.
# In other words, you should run the script like so:
#   cd /path/to/where/I/want/to/store/the/files
#   /path/to/script.sh --partition owners
# (Or whatever partition you want to use.)

# People who have access to the "owners" partition should probably use that,
# to avoid loading down their own nodes.  Everyone else will have to use the
# "normal" partition.

# The script will tell you what to do.  Just run it as above.
# That's it!
# Everything below this line is code, and default configuration.

# Some SLURM parameters.
# We use some safe defaults here.

# Set how long you think the transfer will take.
# This script will resubmit itself if needed
#SBATCH --time=4:00:00

# The AWS CLI does use multithreading for S3 operations.
# So, you can give it multiple CPUs.
# But, compute node bandwidth is limited.
#SBATCH --cpus-per-task=4

# The AWS CLI does not need much memory.  Say, 1 GB per thread.
#SBATCH --mem-per-cpu=1G

# Tell SLURM to signal us 1 minute before we run out of time.
#SBATCH --signal=B:SIGUSR1@60

# When should we be emailed about the job?
#SBATCH --mail-type=BEGIN,END,FAIL

# Now the shell script code begins!

# Try looking for the AWS command
AWS_PATH=$(which aws 2>/dev/null)
if [ ${AWS_PATH:-0} == "0" ]; then
    echo "Could not find \`aws\`!"
    echo "You may have to install the AWS CLI, or modify your PATH."
    echo "Please do what is needed to make the \`aws\` command availabe, and try again."
    exit 1
else
    echo "Using \`aws\` at ${AWS_PATH}"
fi
export AWS_PATH

# Now, some functions.

# This function takes a "NAME=VALUE" string, identifies certain variables that
# we want, and puts their values into the environment.
# Return code has no meaning.
function process_export_line() {
    # The line should be of the format "export NAME=VALUE"
    # Our one parameter is the "NAME=VALUE" part.
    IFS='=' read -r var_name var_value <<< "${1}"

    # Check the variable name against what we want.
    case "${var_name:-X}" in
    AWS_ACCESS_KEY_ID)
        export AWS_ACCESS_KEY_ID=${var_value}
        ;;
    AWS_SECRET_ACCESS_KEY)
        export AWS_SECRET_ACCESS_KEY=${var_value}
        ;;
    AWS_SESSION_TOKEN)
        export AWS_SESSION_TOKEN=${var_value}
        ;;
    esac
    return
}

# This function takes an array of words.
# We expect the format "aws s3 sync URL ."
# We want URL, which we put into the environment at $S3_URL
# Return code has no meaning.
function process_aws_line() {
    # We need five parameters
    if [ $# -ne 5 ]; then
        return
    fi

    # The first three words should be "aws s3 sync"
    # The fifth word should be "."
    if [ "${1}" != 'aws' -o \
         "${2}" != 's3' -o \
         "${3}" != 'sync' -o \
         "${5}" != '.' \
    ]; then
        return
    fi

    # We appear to have a matching string!
    # Set parameter 4 into the S3_URL environment variable
    export S3_URL="${4}"
    return
}

# This reads in a Biohub AWS sync script, parsing out the stuff we want.
# We extract the AWS authentication environment variables, and the S3 URL.
# Takes no parameters.  Return code has no meaning.
function read_and_parse_input() {
    read_exit_code=0
    while [ $read_exit_code -eq 0 ]; do
        # Read a line of input
        read -e -r -a LINE
        read_exit_code=$?

        # Check the read exit code
        # If we got a 1, that's EOF
        if [ ${read_exit_code} -eq 1 ]; then
            # Skip the loop, but do not exit.
            continue

        # If we got a 130, that's an exit
        elif [ ${read_exit_code} -eq 130 ]; then
            echo 'Goodbye!'
            exit 0

        # For everything else that's non-zero, something is weird!
        elif [ ${read_exit_code} -ne 0 ]; then
            echo "Sorry, we got an unexpected code ${read_exit_code} from read."
            exit 1
        fi

        # Is the line an "export"?
        if [ ${LINE[0]:-#} = 'export' ]; then
            # Just send the word after "export"
            process_export_line ${LINE[1]}
        fi

        # Is the line an "aws"?
        if [ ${LINE[0]:-#} = 'aws' ]; then
            # Yes, we send the entire array through.
            process_aws_line "${LINE[@]}"
        fi
    done
    echo 'EOF received!'

    # Did we get all the variables we need?
    if [ "${AWS_SESSION_TOKEN:-X}" = "X" ]; then
        echo 'ERROR!  The AWS_SESSION_TOKEN variable was not found.'
        echo 'Maybe your "export AWS_SESSION_TOKEN" lines were commented out?'
        echo 'Please check your input, and try again.'
        exit 1
    elif [ "${AWS_SECRET_ACCESS_KEY:-X}" = "X" ]; then
        echo 'ERROR!  The AWS_SECRET_ACCESS_KEY variable was not found.'
        echo 'Maybe your "export AWS_SECRET_ACCESS_KEY" lines were commented out?'
        echo 'Please check your input, and try again.'
        exit 1
    elif [ "${AWS_ACCESS_KEY_ID:-X}" = "X" ]; then
        echo 'ERROR!  The AWS_ACCESS_KEY_ID variable was not found.'
        echo 'Maybe your "export AWS_ACCESS_KEY_ID" lines were commented out?'
        echo 'Please check your input, and try again.'
        exit 1
    elif [ "${S3_URL:-X}" = "X" ]; then
        echo 'ERROR!  The "aws s3 sync" command was not found.'
        echo 'Maybe your "aws s3 sync" lines were commented out?'
        echo 'Please check your input, and try again.'
        exit 1
    fi

    # We're done!
    return
}

# Check our AWS credentials and S3 URL by doing an `ls`.
# Takes no parameters.  Uses the environment to try doing an S3 listing.
# Exits if there is a problem.
# Return code has no meaning.
function check_credentials() {
    ls_output=$($AWS_PATH s3 ls "${S3_URL}" --recursive --page-size 10 < /dev/null 2>&1)
    if [ $? -ne 0 ]; then
        echo "ERROR!  Our attempt to call \`${AWS_PATH} s3 ls ${S3_URL}\` failed."
        echo "There is probably a problem with your credentials."
        echo "Here is the output we received from the command:"
        echo "${ls_output}"
        exit 1
    fi
    return
}

# Now we have live code!

# Are we running outside of a job?
if [ "${SLURM_JOBID:-X}" = "X" ]; then
    # We are running outside of a job.
    echo 'Hello!'

    # Remind about the download location.
    echo ''
    echo "This script will place all download files in `pwd`"
    echo "If that is the wrong place, then press Control-C to exit, \`cd\` to the correct place, and run this script again!"

    # Time to read input!
    echo ''
    echo 'Please paste the download script (the .sh file) now.'
    echo 'You can paste the entire file.'
    echo 'When done, send an EOF.'
    echo '(Press Return (or Enter) once, and then press Control-D.)'
    echo 'To exit, press Control-C.'
    echo 'Waiting for input...'
    read_and_parse_input

    # Check credentials
    echo 'Checking AWS credentials...'
    check_credentials

    # Checks complete!
    echo 'Everything looks good!'
    echo 'Submitting ourselves as a SLURM job...'
    echo '(You should get mail when the job starts, and completes or fails.)'
    sbatch $* $0

else
    # We are running in a SLURM job.

    # When Bash (the shell running the job script) gets a USR1 signal, ask
    # SLURM to requeue us.
    _requeue() {
        scontrol requeue ${SLURM_JOBID}
    }
    trap '_requeue' SIGUSR1

    # Start the S3 sync in the background, and wait for it to complete.
    # THIS IS IMPORTANT!
    # If we just run the `aws` command directly, we won't catch the "Your job
    # is ending" signal in time.  We have to run the `aws` command in a
    # separate process, so that the signal can be caught.
    $AWS_PATH s3 sync ${S3_URL} . --only-show-errors \
        < /dev/null 1> /dev/stdout 2> /dev/stderr &
    wait

    # Get the exit code from the `aws` command, and exit with that code.
    aws_exitcode=$?
    exit ${aws_exitcode}
fi
