import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from subprocess import run, CalledProcessError

# Function to execute a notebook
def execute_notebook(notebook):
    start_time = time.time()
    try:
        print(f"Starting {notebook} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}...")
        result = run(["jupyter", "nbconvert", "--inplace", "--execute", notebook], check=True)
        end_time = time.time()
        print(f"Completed {notebook} in {end_time - start_time:.2f} seconds")
    except CalledProcessError as e:
        print(f"Error executing {notebook}: {e}")

notebook_groups = {
    'Group 1' : ['bit_00_weekly_variables.ipynb', 'bit_01_master_profile.ipynb', 'bit_02_calls_samples_abbv.ipynb'],
    'Group 2' : ['bit_03_Geosummary_Sales_Activity.ipynb', 'bit_04_Geosummary_Sales_KPI.ipynb', 'bit_05_Geosummary_Sales_Performance.ipynb', ],
    'Group 3' : ['bit_06_Geosummary_Trend_Feed.ipynb','bit_07_Prescriber_Profile_Info.ipynb', 'bit_08_Prescriber_Sales_Performance.ipynb'],
    'Group 4' : ['bit_09_Prescriber_Trend_Feed.ipynb', 'bit_10_Prescriber_Sales_Activity.ipynb','bit_11_Prescriber_PayerMix.ipynb'],
	'Group 5' : ['bit_13_DenormalizedPrescriber_ProfileInfo.ipynb', 'bit_14_DenormalizedPrescriber_MetricPerformance.ipynb', 'bit_15_Doc360_CallActivity.ipynb'],
    'Group 6' : ['bit_16_Doc360_MC.ipynb', 'bit_16_Doc360_SampleActivity.ipynb', 'bit_17_LinzessSnapshot_ProfileInfo.ipynb'],
	'Group 7' : ['bit_18_LInzessSnapshot_SalesActivity.ipynb', 'bit_19_LinzessSnapshot_Sales_Performance.ipynb', 'bit_20_Prescriber_Payer.ipynb']
}

max_workers = 4 # Set the number of workers

# Track overall progress
total_notebooks = sum(len(files) for files in notebook_groups.values())
completed_notebooks = 0

# Run each group of notebooks in parallel
for group, files in notebook_groups.items():
    group_start_time = time.time()
    print(f"\nRunning {group} with {len(files)} notebooks...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(execute_notebook, notebook) for notebook in files]
        for future in as_completed(futures):
            future.result()  # This will raise any exceptions that occurred during execution
            completed_notebooks += 1
            print(f"Progress: {completed_notebooks}/{total_notebooks} notebooks completed")

    group_end_time = time.time()
    print(f"Completed {group} in {group_end_time - group_start_time:.2f} seconds")

print("All notebook groups have been executed.")
