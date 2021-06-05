

<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://github.com/JakoCrix/CloudDatabase_Odin">
    <img src="admin_repo/md_pic.png" alt="Logo" width="80" height="80">
  </a>
  <h3 align="center">CloudDatabase_Odin</h3>
  <p align="center">
    Odin on the Cloud!
    <br />
    <a href="https://github.com/JakoCrix/CloudDatabase_Odin"><strong>Explore the docs »</strong></a>
  </p>
</p>

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-the-project"> About the Project</a>
    <li><a href="#getting-started">Getting Started</a></li>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    <li><a href="#examples">Usage Example</a></li>
    <li><a href="#road-map">Roadmap</a></li>
      <ul>
        <li><a href="#odin-cloud-v1">Odin Cloud v1</a></li>
        <li><a href="#odin-cloud-v2">Odin Cloud v2</a></li>
        <li><a href="#odin-cloud-v3">Odin Cloud v3</a></li>
      </ul>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>

<!-- Introductory Section -->
## About The Project
Odin is a relational database built with the objective of providing Reddit information along with an emphasis on tracking the lifecycles of reddit submission and comments. This creates an additional point of contact layer that identifies how the reddit community is interacting and reacting to submissions and comments.  

This repository is geared towards migrating the Odin architecture from a local database [Odin-SQLite](https://github.com/user/repo/blob/branch/other_file.md) into a cloud architecture utilizing cloud operating services at a cost efficient manner.  

Odin has a focus on **financial information** on reddit which have an impact the following subreddits and markets: 
1. NYSE: r/stocks, r/StockMarket, r/investing, r/wallstreetbets, r/ wallstreetbets, r/pennystocks
2. ASX: r/asx_bets, r/ausstocks, r/ASX

<!-- Getting Started -->
## Getting Started
This repo is a progression from Odin [Odin-SQLite](https://github.com/user/repo/blob/branch/other_file.md) which already outlines the ETL process beteween Reddit and the end database. 

### Prerequisites
For cloud service provider, we decided to go for Microsoft Azure because of its cost effectiveness: further outlined in the Major Decisions section. Main Azure services used:  
1. Azure, SQL Database: Cost effective relational database option with potential to migrate to no NoSQL. 
2. Azure, Keyvault: A godsend!! Super effective secret storage. 
3. Azure, Azure Functions: Allows automating Python ETL process into a cloud container.

### Installation
I provided both a YAML file (Conda environment) and a requirements.txt (pip environment) file in the repo for environment cloning. 

Envi Setup through Pip: 
```sh
cd ~/CloudDatabase_Odin
pip install -r requirements.txt
```

Envi Setup through YAML(conda): 
```sh
cd ~/CloudDatabase_Odin
conda env create > environment.yml
```

<!-- Usage Examples-->
## Examples
I included some T-SQL scripts for a quick dive as well as some details of usecase examples. For database access, please reach out to @JakoCrix to set authentication. 

<!-- Prod Releases-->
## Road Map
#### Odin Cloud v1
2021/03/19
- Cloned local SQLite Database onto Microsoft Azure. Decided on a general purpose serverless option with 1 vcore; heaviest logged job requires 80 DTU's which is approx 1 vcore.  
- Automated reddit scraping process onto the cloud using Azure functions; running ETL jobs 4 times a day.  
- Rewrote the ETL process to pipe data into Odin on the cloud. 

#### Odin Cloud v2
2021/04/01
- Moved from Microsoft serverless option to standard option because of server cost. Specifically, server only stops after an hour of inactivity. Compiled with ETL scraping, the server is on unnecessarily on for half a day :(. 
- Re-Rewrote the ETL process to more efficiently move data into the cloud- capped to a limit of 10 DTU's (Tradeoff of execution speed for significant cost savings). Multiple tiny injections instead of a singular data injection. 
- New tables added and ETL processes created for additional financial information scraping (ticker names). 

#### Odin Cloud v3
- Potential migration to NoSQL database. 

<!-- Contact-->
### Contact
Github handle: JakoCrix
Project Link: https://github.com/JakoCrix/CloudDatabase_Odin