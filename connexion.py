from pymongo import MongoClient
import json
import os

client = MongoClient('mongodb://localhost:27017')
db = client["dblp"]
collec = db.publis
n=1


def deco():
    global n
    print("+++++++++++++++++++++++++++++++++++")
    print("+++++++++++++++++++++++++++++++++++")
    print("+++++++++       ", n,"       +++++++++")
    print("+++++++++++++++++++++++++++++++++++")
    print("\n")
    n+=1

def pause():
    print("\n")
    programPause = input("Appuyez sur <ENTREE> pour continuer...")
    deco()

deco()
# Compter le nombre de documents de la collection publis ;
results1 = db.publis.count_documents({})
print("il y a ", results1, " documents dans la collection")

pause()

# Lister tous les livres (type “Book”) ;
print("Liste des livres : \n")
results2 = list( collec.find({"type":"Book"}) )
for i in range(10) :
	print(",".join(results2[i]["authors"]))
	print("|{:^10}|{:<100}|{:^10}|".format(results2[i]["type"], results2[i]["title"], results2[i]["year"]))

pause()

# Lister les livres depuis 2014 ;
print("Liste des livres parus depuis 2014 : \n")
results3 = list( collec.find({"type":"Book", "year":{"$gte":2014}}) )
for i in range(10) :
	print(",".join(results3[i]["authors"]))
	print("|{:^10}|{:<100}|{:^10}|".format(results3[i]["type"], results3[i]["title"], results3[i]["year"]))

pause()

# Lister les publications de l’auteur “Toru Ishida” ;
print("Publications de Toru Ishida : \n") 
results4= db.publis.find({"authors":{'$regex':'Toru Ishida'}}, {"type":1, "title":1, "year":1, "_id":0})
for result in results4 :
	print("|{:^10}|{:<100}|{:^10}| \n".format(result["type"], result["title"], result["year"]))

pause()

# Lister tous les auteurs distincts ;
print("Liste des auteurs : \n") 
result5 = collec.distinct("authors")
for i in range(25) :
	print(result5[i]) ## on imprime que les 20 premiers

pause()

## Trier les publications de “Toru Ishida” par titre de livre ;
print("Tri des publications de Ishida par titre : \n") 
results6 = collec.aggregate( [ {"$match":{"authors":"Toru Ishida"}}, {"$sort":{"title":1}} ] )
for result in results6 :
	print(",".join(result["authors"]))
	print("|{:^10}|{:<100}|{:^10}|".format(result["type"], result["title"], result["year"]))

pause()

# Compter le nombre de ses publications ;
results7= db.publis.count_documents({"authors":{'$regex':'Toru Ishida'}})
print(" >>>>    Toru Ishida a publié : ", results7, "articles  <<<<")

pause()

# Compter le nombre de publications depuis 2011 et par type ;
print("Nombre de publications depuis 2011 par type : \n") 
results8 = list(
	collec.aggregate( [ 
	{"$match":{"year":{"$gte":2011}}}, 
	{"$group":{"_id" : "$type", "total":{"$sum":1}}} 
	] )
	)
for result in results8 :
	print("|{:<50}|{:^10}|".format(result["_id"], result["total"]))

pause()

# Compter le nombre de publications par auteur et trier le résultat par ordre croissant ;
print("Nombre de publications par auteur, dans l'odre croissant : \n") 
results9 = list(
	collec.aggregate( [  
	{"$unwind":"$authors"},
	{"$group":{"_id" : "$authors", "total":{"$sum":1} } },
	{"$sort":{"total":1}},
	] )
	)
for i in range(len(results9)-20, len(results9)):
    print("|{:<50}|{:^10}|".format(results9[i]["_id"], results9[i]["total"]))

