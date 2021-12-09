# Compile usage statisitcs from bookings in a calendar exported to xls files

This notebook compiles statistics from a booking calendar exported to a set of excel xlsx files.

## Step by step installation of python work environment (tested on Windows 10)

The following instruction for people who never used python before.

- Download miniconda from https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe
- Run the installer, find a folder without sapce to install it C:\Software\Miniconda3
- Download Visual Studio Code https://code.visualstudio.com/sha/download?build=stable&os=win32-user
- Open Visual Code and on the start page, select clone repository. At the top, copy paste the link to this repository (https://github.com/jboulanger/UsageStats)
- Open the file '' and install python extension  select python interpreter in C:\System\Miniconda3\ when prompted.
- Create an python environement. In Visual Code:
    - Press terminal > new terminal (select a cmd terminal not Powershell)
    - Type ```C:\Software\Miniconda3\Scripts\activate C:\Software\Miniconda3\``
    - The terminal line should start by ```(base)```
    - Create the environment ```conda env create -f environment.yml```
    - Activate the environement ```conda activate usagestats```, now the terminal should line should start with ```(usagestats)```.
    - Register the kernel for notebooks: ```python -m ipykernel install --user --name usagestats```
    - Select the kernel at the top right of the notebook.
    - We are ready to run the notebook

## Configuration
The notebook should be at the same level than xlsx files, for simplicity copy the files next to the notebook.

## Usage
- In visual code open the file 'Usage Stats Analytics.ipynb' as a jupyter notebook.
- Configure the input in the first cell and run all the subsequent cells (Run all) to produce the graphs and csv files.
- If no file 'users.csv' and 'groups.csv' are present use they will be created with 'Unknown' values for groups and divisions. Check the files users.csv and groups.csv and fill out the missing values.
- Run again the notebook to update the graphs.


## Note
The git repository remove the output of the notebook when staging files using a .gitattibutes files and add the following line in the .git/config file
```
[filter "strip-notebook-output"]
    clean = "jupyter nbconvert --ClearOutputPreprocessor.enabled=True --to=notebook --stdin --stdout --log-level=ERROR"
```

The environment.yml file was created using:
```
conda env export --no-builds  --from-history | head -n -1 > environement.yml
```
to remove the site specific prefix line.