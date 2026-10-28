# Usage statistics from bookings in kerio calendar

These notebook compiles statistics from a booking calendar.

## Booking rules for users

- Between the hours of 9am to 6pm (Monday to Friday):
    - No more than 3 hours per booking.
    - Do not sign up for more than 3 slots per microscope in advance (including overnight runs that stretch into the 9-6 time zone).
- Last minute sign up, on the day
    - no restrictions (Don't forget to sign up in the Kerio Calendar).
- Evening and weekends:
    - No restrictions on slot duration but do not sign up for more than 3 overnight runs and 3 whole days at weekends in advance per microscope

## Booking rules for light microscopy team

The code analyses the calendar and categorizes the bookings based on 3 keywords  (maintenance, service, training) in the title. 

1. When booking a system for anything else than a service of this system, a training on this system or using the system for imaging, use the keyword **maintenance**.
2. When booking a system for a service visit from an engineer, use **service**.
3. When booking a system for training a user, use **training**.
4. Do not book a session for a user in your name.


## Step by step installation of python work environment (tested on Windows 10)

The following instruction are for people who never used python before.

### Installing python
To run the code, we need python. To install python you can follow these steps:

- Download miniconda from https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe
- Run the installer, find a folder without spaces to install it C:\Software\Miniconda3

### Installing Visual Studio code

We can use Visual Studio code as an integrated environment for running the code. We could also use jupyter notebook or jupyter lab.
- Download Visual Studio Code https://code.visualstudio.com/sha/download?build=stable&os=win32-user
- Open Visual Code and on the start page, select clone repository. At the top, copy paste the link to this repository (https://github.com/jboulanger/UsageStats)
- Open the file 'Usage Stats Analytics.ipynb'
- Install python extension and select python interpreter in C:\System\Miniconda3\ when prompted.

### Create an environement

The environment insures that you have all the necessary packages and isolate this configuration for the rest of the system.

In Visual Code or in (conda) terminal:
- Press terminal > new terminal (select a cmd terminal not Powershell)
- Type ```C:\Software\Miniconda3\Scripts\activate C:\Software\Miniconda3\```
- The terminal line should start by ```(base)```
- Create the environment ```conda env create -f environment.yml```
- Activate the environement ```conda activate usagestats```, now the terminal should line should start with ```(usagestats)```.
- Register the kernel for notebooks: ```python -m ipykernel install --user --name usagestats```

## Usage

### Create a database
The first step is to fetch the data from kerio and store it in an sql database. The database has several tables, that are created by loading csv files that are convenient to edit manually.

- calendars and instruments are created by inspecting the file 'calendards.csv'
- divisions and groups are created from groups.csv
- users are created from 'users.csv'
- events are created from loading the calendars from kerio.


To create the database:
1. Create a new folder for the current year
2. Copy the tables 'users.csv' and 'group.csv' and 'calendar.csv'
3. Connect to kerio with you credentials
3. Open one of the URL in the calendar.csv
3. Open the dev tool (ctrl+shift+I) and copy the curl command from the network tab, right click on line of the table, copy value, copy cURL and paste the content in a file 'cookie.txt'.
4. Open the notebook '1_Create Database.ipynb'
5. Set the folder path at the top of the notebook
6. Run the cells to create a 'bookings.db' sql database file in the folder with the content of the calendars.

### Add missing users and group

If there are groups and users which are new, they will not be associated to the corresponding division and groups and need to be manually.
- Open the notebook '2_add users.ipynb'
- Edit the start data
- Run the first cell 
- Edit the file 'new users.csv'
- Run the last cell to update the database with new users.

### Create the reports

- Run the notebook '3_Create report.ipynb' to create all the reports.

## Notes

### Notebook output

Notebooks might contain personal information (name/email etc) that need to be stripped out before push on github.

The git repository configuration removes the output of the notebook when staging files using a .gitattibutes files and by having the following line in the file .git/config:
```
[filter "strip-notebook-output"]
    clean = "jupyter nbconvert --ClearOutputPreprocessor.enabled=True --to=notebook --stdin --stdout --log-level=ERROR"
```

The environment.yml file can be recreated using:
```
conda env export --no-builds  --from-history | head -n -1 > environment.yml
```

### Duplicates
The calendars have GUID that 'uniquely' identify booking events. 
However, modified events and group booking share the same GUID.

### Browsing the database manually
The database can be inspected manually using:
```bash
sqlitebrowser bookings.db
``` 