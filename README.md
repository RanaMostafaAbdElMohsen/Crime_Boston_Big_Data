## Crime_Boston_Big_Data
This is a project, Crime Boston Project, intended for Big Data course at Cairo University

### Prequisities for Setting Up Environment

- Preferably work with `linux`
- Download `Flink version 1.9.0`
- Make sure you have `Java 1.8` installed
- Install maven package:
    - `sudo apt update`
    - `sudo apt install maven`
- Verify maven package is correctly installed
    - `mvn -version`


### Instructions for running project on flask successfully

#### Getting Started

- git clone `https://github.com/RanaMostafaAbdElMohsen/Crime_Boston_Big_Data.git`
- Use your favourite IDE `Intellji` or `VSCode`
- Navigate to `crime-boston/src/main/java/Insights/`
- For each task assigned, create a new .java class
- Open terminal `cd crime-boston`
- Build project `using mvn clean package` -> jar file

#### Pushing upstream Repo

- For each task u need to do the following:
        - git commit changes
        - git push in a new branch
        - Open a PR + assign reviewers
        
- Take output and perform some visualization ( pie-chart, bar chart, etc...)
- Save these images

#### Running Flink Useful links

- This tutorial is very useful for running flink : `https://ci.apache.org/projects/flink/flink-docs-release-1.9/getting-started/tutorials/local_setup.html`
- Flink Apache Framework Presentation : `https://docs.google.com/presentation/d/1jLk0ZnmKzDRbkCVgEAwbNSTzcfz1VsIFR9v2RiRljfE/edit?usp=sharing`
- Flink Big Data Sample Repository ( Word Count ) (Presentation Example ) : `https://drive.google.com/drive/folders/17rqokzzGHfEwadCKq8pewvVUt5SgmgP-?usp=sharing`
- Useful tutorial for apis supported in 1.9 but explained well in flink v1.2 : `https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/batch/index.html`  
- Crime Boston Flink Project : `https://docs.google.com/presentation/d/1x2Vc4fVjgfmzJvPMLSIqHK69mi5mGEqqgDom4Vr5HYU/edit?usp=sharing`
        
#### Todo Tasks

1- for each day in month get crime rate 

2- for each day for each hour get count of crimes

3- Fe 7tt counting values w unique w null l kol column

4- KNN

5- Remove NULL data, Fill columns that doesn't have value



#### In Progress Tasks
2 - for each hour in year get count of crime rates 

3- for each UCR part get count of offence code


#### Accomplished Tasks

1- get safe district (District That has minimum Number of crimes)

2- get count of crime based on street (For eaxh Street get crime rate)

3- group crimes in street by year (group street crimes per year)

4- get count of previous groupping (For each year for each street get crime rate)

5- Crimes count per Year (Crime rate per year)

6- Hourly Crime Rates by Month

7- Crimes count per day (Crime rate per day)

8- Crimes count per hour (Crime rate per hour)

9- Not Safest Street (Street that has maximum number of crimes )

10- offence_code_per_hour_count (For each hour for each offence code get crime rate)

11- Count crimes per distrcit (Crime rate per district)

12- For each offence code get crime rate

13- Crime count rate for each offence per day

14-Year that has maximum crime rate

15-Day that has maximum crime rate

16-Month that has maximum crime rate

17- Hour that has maximum crime rate

18- for each district count number of different offence codes 

19- For each year for each offence code get crime rate

20- For each year for each month get crime rate

21- For each Month For each offence code get crime rate
