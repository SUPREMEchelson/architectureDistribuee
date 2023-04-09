# architectureDistribuee
![bigDataimage](https://user-images.githubusercontent.com/43779857/202323288-ec72d648-30ab-425e-b9b4-aadce2242500.jpg)

# Description
La recherche d'emploi et de logement est l'une des préocupations principales des francais. L'objectif de ce projet est de voir si il y'a une corrélation entre les bassins à fort concentration d'emploi et le loyer des logements. 

# Architecture

![SchemaProjetBigData](https://user-images.githubusercontent.com/43779857/202323405-14ed0ecb-ed66-4882-a7c9-b6fcfca8e287.jpg)

# Etat de développement
Le projet est en stade de développement.

# Technologie
Kafka
Spark
Docker
MongoDB

# Installation

récupérer le projet :
git clone https://github.com/SUPREMEchelson/architectureDistribuee.git

# Dockerisation
positionner votre cmd à l'emplacement du fichier ```docker-compose.yml``` puis faite ```docker-compose up```

# Traitement Spark
Lancer jupyter à l'aide du 
```localhost 8080```

# Visualisation

La visualisation des données est faites avec l'application microsoft powerBi

# Gestion de projet
https://trello.com/invite/b/GmYVkOpJ/ATTI3805a8c69405feb8add054cd779c13aa37D9F0A6/projetarchitecturedistribuee

# video

[Desmonstration kafka](https://auvencecom.sharepoint.com/teams/GROUPE-perso624/_layouts/15/embed.aspx?UniqueId=49c26acf-3c3b-4649-a88b-c96ce8)

# Faire fonctionner le consumer en ligne de commande

Après avoir exécuter le producer
Ouvrez un cmd
  faites afin d'afficher les machines ``` docker ps```  
  puis rentrez dans la machines kafka ``` docker exec 317 ```  
  pour regarder dans la liste des topics crée ``` kafka-topics.sh --bootstrap-server=localhost:9092 --list```  
  pour consomer le topic kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello_topic --from-beginning  

exemple consumer 
![apiPoleEmloiDonner](https://user-images.githubusercontent.com/43779857/230797465-88e04acf-3b08-48be-8f1d-ae8d0e5336d3.png)



# Auteur

Youssef
Jovickf
Chelson Suprême
