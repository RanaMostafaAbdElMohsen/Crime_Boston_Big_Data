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
        
#### Tasks
1- crime count rate for each offence per day

2- for each day in month get crime rate 

3 - for each hour in year get count of crime rates 

4- for each district count number of different offence codes 

5- for each UCR part get count of offence code

6- for each day for each hour get count of crimes

7- Fe 7tt counting values w unique w null l kol column

8- for ech day for each hour get count of shooting

9- KNN

10- Year that has maximum crime rate

11- Hour that has maximum crime rate

12- Month that has maximum crime rate

13- Day that has maximum crime rate


