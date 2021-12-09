# Compile usage statisitcs from bookings in a calendar exported to xls files

This notebook compiles statistics from a booking calendar exported to a set of excel xlsx files.

## Step by step installation of python work environment (tested on Windows 10)

The following instruction for people who never used python before.

- Download miniconda from https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe
- Run the installer, find a folder without sapce to install it C:\Software\Miniconda3
- Download Visual Studio Code https://code.visualstudio.com/sha/download?build=stable&os=win32-user
- Open the notebook and install python extension  select python interpreter in C:\System\Miniconda3\ when prompted.
- Create an python environement
    - Press terminal > new terminal (select a cmd terminal not Powershell)
    - Type ```C:\Software\Miniconda3\Scripts\activate C:\Software\Miniconda3\``
    - The terminal line should start by (base)
    - Create the environment ```conda create -n usagestats```
    - Activate it : ```conda activate usagestats```
    - Install packages : ```conda install pandas matplotlib seaborn ipykernel```
    - Register the kernel for notbooks: ```python -m ipykernel install --user --name usagestats```
    - Select the kernel at the top right of the notebook.
    - We are ready to run the notebook

## Configuration
The notebook should be at the same level than xlsx files.

## Usage
- Open the notebook
- Configure the input in the first cell and run all the subsequent cells (Run all) to produce the graphs and csv files.
- If no file 'users.csv' and 'groups.csv' are present use they will be created with 'Unknown' values for groups and divisions. Check the files users.csv and groups.csv and fill out the missing values.
- Run again the notebook to update the graphs.


## Note
The git repository remove the output of the notebook when staging files using a .gitattibutes files and add the following line in the .git/config file
```
[filter "strip-notebook-output"]
    clean = "jupyter nbconvert --ClearOutputPreprocessor.enabled=True --to=notebook --stdin --stdout --log-level=ERROR"
```