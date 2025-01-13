#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Function to display usage
show_help() {
    echo "Usage: ./run_snapshot.sh [OPTIONS]"
    echo "Options:"
    echo "  --reset    Reset the database before starting"
    echo "  --token    Token to process (default: AERO)"
    echo "Example:"
    echo "  ./run_snapshot.sh --reset --token AERO"
}

# Parse command line arguments
ARGS=""
for arg in "$@"; do
    ARGS="$ARGS $arg"
done

# Run the Python script with arguments
python snapshot.py $ARGS

# Deactivate virtual environment
deactivate
