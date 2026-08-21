# This script is used to check the crab statuses of the jobs. Given a base directory, it will check the status of all crab jobs in that directory and print the output to the console. This is useful for monitoring the progress of the jobs and identifying any issues that may arise during execution.
#!/usr/bin/env python3
import subprocess
import sys
import os
import re
import argparse
def check_crab_status(crab_dir):
    try:
        result = subprocess.run(['crab', 'status', crab_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            print(f"Error checking status for {crab_dir}: {result.stderr}")
            return None
        return result.stdout
    except Exception as e:
        print(f"Exception occurred while checking status for {crab_dir}: {e}")
        return None

if __name__ == "__main__":
    argument_parser = argparse.ArgumentParser(description="Check the status of crab jobs in a given directory.")
    argument_parser.add_argument("-d", "--directory", help="The base directory containing the crab job directories", required=True)
    argument_parser.add_argument("--resubmit", action="store_true", help="Resubmit failed jobs after checking status")
    argument_parser.add_argument("--removeCompleted", action="store_true", help="Remove completed jobs after checking status")
    argument_parser.add_argument("--removeSubmitFailed", action="store_true", help="Remove jobs that were not submitted successfully after checking status")
    argument_parser.add_argument("--killRunning", action="store_true", help="Kill running jobs after checking status")
    argument_parser.add_argument("--include", help="Regex pattern: only check directories whose names match this pattern")
    argument_parser.add_argument("--exclude", help="Regex pattern: skip directories whose names match this pattern")
    args = argument_parser.parse_args()


    base_directory = args.directory
    crab_dirs = subprocess.run(['ls', base_directory], stdout=subprocess.PIPE, text=True).stdout.splitlines()
    # Remove any files from the list, we only want directories
    crab_dirs = [d for d in crab_dirs if os.path.isdir(os.path.join(base_directory, d))]

    for crab_dir in crab_dirs:
        # Check if the directory name starts with "crab_" to ensure we are only checking crab job directories
        if crab_dir.startswith("crab_"):
            if args.include and not re.search(args.include, crab_dir):
                continue
            if args.exclude and re.search(args.exclude, crab_dir):
                continue
            full_path = f"{base_directory}/{crab_dir}"
            print(f"Checking status for {full_path}...")
            status_output = check_crab_status(full_path)
            # extract the %ge of jobs finished from the status output
            if status_output is not None:
                # Check if job was submitted OR not
                if "SUBMITREFUSED" in status_output:
                    print(f"Job {full_path} was not submitted.")
                    if args.removeSubmitFailed:
                        print(f"Removing job {full_path} that was not submitted successfully...")
                        remove_result = subprocess.run(['rm', '-rf', full_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        if remove_result.returncode != 0:
                            print(f"Error removing job {full_path}: {remove_result.stderr}")
                        else:
                            print(f"Successfully removed job {full_path}")
                    continue
                # Find the content starting from 'Jobs status:' and before 'No publication'
                start_index = status_output.find("Jobs status:")
                end_index = status_output.find("No publication")
                if start_index != -1 and end_index != -1:
                    jobs_status = status_output[start_index:end_index].strip()
                    print(f"Status for {full_path}:\n{jobs_status}")
                else:
                    print(f"Could not extract job status for {full_path}.")
                    continue
                if args.resubmit:
                    if "failed" in jobs_status.lower():
                        print(f"Resubmitting failed jobs for {full_path}...")
                        # NOTE: `crab resubmit <path>` (bare positional arg) fails; the
                        # CRAB client here only accepts the directory via -d. Confirmed
                        # directly: -d works, bare positional does not.
                        resubmit_result = subprocess.run(['crab', 'resubmit', '-d', full_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        if resubmit_result.returncode != 0:
                            print(f"Error resubmitting jobs for {full_path}:\nstdout: {resubmit_result.stdout}\nstderr: {resubmit_result.stderr}")
                        else:
                            print(f"Successfully resubmitted jobs for {full_path}")
                if args.removeCompleted:
                    # Check if this `finished                100.0%` is in the status
                    if "finished" in jobs_status.lower():
                        if "100.0%" in jobs_status:
                            print(f"Removing completed jobs for {full_path}...")
                            remove_result = subprocess.run(['rm', '-rf', full_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                            if remove_result.returncode != 0:
                                print(f"Error removing completed jobs for {full_path}: {remove_result.stderr}")
                            else:
                                print(f"Successfully removed completed jobs for {full_path}")
                if args.killRunning:
                    if "running" in jobs_status.lower():
                        print(f"Killing running jobs for {full_path}...")
                        kill_result = subprocess.run(['crab', 'kill', full_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        if kill_result.returncode != 0:
                            print(f"Error killing running jobs for {full_path}: {kill_result.stderr}")
                        else:
                            print(f"Successfully killed running jobs for {full_path}")