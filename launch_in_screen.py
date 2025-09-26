import subprocess
import os

# Create a sample script to run
script_path = "/tmp/test_screen_script.sh"
with open(script_path, "w") as f:
    f.write("#!/bin/bash\n")
    f.write("echo 'Hello from inside the screen session!'\n")
    f.write("sleep 60\n")  # Keep it alive for a bit to test
    f.write("echo 'Done.'\n")

# Make the script executable
os.chmod(script_path, 0o755)

# Define the screen session name
session_name = "test_screen"

# Construct the screen command
screen_command = f"screen -dmS {session_name} bash -c '{script_path}'"

# Run the command
subprocess.run(screen_command, shell=True)

print(f"Screen session '{session_name}' started with script '{script_path}'.")
print(f"Attach using: screen -r {session_name}")
