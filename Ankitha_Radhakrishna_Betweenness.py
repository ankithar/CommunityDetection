
# coding: utf-8

# In[ ]:


#import findspark
#findspark.init()

import os, sys
from pyspark import SparkContext
import time
import math
import Queue
import collections

import networkx as nx
from networkx import number_connected_components
from networkx import connected_component_subgraphs
from networkx import connected_components

start = time.time()
sc = SparkContext(appName="AnkithaR")

#inputFile = "E:/USC/DataMining/Assignment/Assignment4/Assignment_04/Description/data/ratings.csv"
#inputFile = "ratings.csv"
inputFile = sys.argv[1]
fileContents = sc.textFile(inputFile)
RDD = fileContents.zipWithIndex().filter(lambda (row,index): index > 0).keys().map(lambda x:x.split(','))
RDDUserToMovie = RDD.map(lambda x : (int(x[0]),int(x[1]))).groupByKey().sortByKey().map(lambda x : (x[0],set(x[1]))).collect()

listOfEdges = []
lenOfRDDUserToMovie = len(RDDUserToMovie)
for i in range(lenOfRDDUserToMovie-1):
    for j in range(i+1,lenOfRDDUserToMovie):
        commonMovies = RDDUserToMovie[i][1] & RDDUserToMovie[j][1]
        if len(commonMovies) >= 9:
            listOfEdges.append((RDDUserToMovie[i][0],RDDUserToMovie[j][0]))
        

def BFSRoot(rootNode):
    #print "BFSRoot for %d " % rootNode
    BFSInfo = {}
    #for node in GraphBFSTest.nodes:
    for node in GraphBFS.nodes:
        BFSInfo[node] = [0,0,[],0] #[visited,level,listofparents,numberOfPaths]
    BFSQueue = Queue.Queue()
    BFSStack = Queue.LifoQueue()
    nodesCreditContribution = {}
    BFSQueue.put(rootNode)
    #BFSInfo[5].extend((1,0,[5],1))
    listTemp = [1,0,[rootNode],1]
    BFSInfo[rootNode] = listTemp
    while not BFSQueue.empty():
        parent = BFSQueue.get()
        BFSStack.put(parent)
        nodesCreditContribution[parent] = 0
        levelOfParent = BFSInfo[parent][1]
        #print "parent,level"
        #print parent,levelOfParent
        numOfPathsOfParent = BFSInfo[parent][3]
        #print "number of paths of parent = "
        #print numOfPathsOfParent
        #for adjacentNode in GraphBFSTest.neighbors(parent):
        for adjacentNode in GraphBFS.neighbors(parent):
            '''
            print "child"
            print adjacentNode
            print "len BFSInfo[adjacentNode]"
            print len(BFSInfo[adjacentNode])
            '''
            #if len(BFSInfo[adjacentNode]) == 0: #child not visited yet
            if BFSInfo[adjacentNode][0] == 0:   #child not visited yet
                BFSQueue.put(adjacentNode)
                #print "parent check"
                #print parent
                listTemp = [1,(levelOfParent + 1),[parent], 1]
                #BFSInfo[adjacentNode].extend((1,(levelOfParent + 1),[parent]))
                BFSInfo[adjacentNode] = listTemp
                #CHECK
                #tupleEntry = orderTuple(parent,adjacentNode)
                #edgeWtDict[tupleEntry] = 0.0
                
                
            else:
                levelOfChild = BFSInfo[adjacentNode][1]
                if levelOfChild > levelOfParent:
                    #print "parentListCheck"
                    #print BFSInfo[adjacentNode][2]
                    BFSInfo[adjacentNode][2].append(parent)
                    BFSInfo[adjacentNode][3] += 1
                    #CHECK
                    #tupleEntry = orderTuple(parent,adjacentNode)
                    #edgeWtDict[tupleEntry] = 0.0
            '''
            print "new BFSInfo"
            print BFSInfo
            print "======================================================================="
            print edgeWtDict
            '''
    #if(rootNode == 102):
    #    fTemp = open("E:/USC/DataMining/Assignment/Assignment4/tmp/val.txt",'w')
        
        
        dictTemp = {}
        for i in BFSInfo.keys():
            dictTemp[i] = BFSInfo[i][3]
        '''    
        fTemp.write(str(dictTemp))
        fTemp.close()
        print "************************"
        print "VAL"
        print dictTemp
        print "************************"
        '''
    
    #print "Calculating weights"
    #print "==========="
    #Calculating edge weights
    #dictOfWt = {}
    while not BFSStack.empty():
        
        nodeToBeExamined = BFSStack.get()
        #print "nodeToBeExamined"
        #print nodeToBeExamined
        if nodeToBeExamined != rootNode:
            nodeCredit = float(1 + nodesCreditContribution[nodeToBeExamined])/float(BFSInfo[nodeToBeExamined][3])
            #if rootNode == 76:
             #   dictOfWt[nodeToBeExamined] = nodeCredit
            #print "credit of %d = %d" % (nodeToBeExamined,nodeCredit)
            for p in BFSInfo[nodeToBeExamined][2]:
                #print "parent"
                #print p
                tupleTemp = orderTuple(p,nodeToBeExamined)
                edgeWtDict[tupleTemp] += nodeCredit
                nodesCreditContribution[p] += nodeCredit
                #nodesCreditContribution[p] += nodeCredit * BFSInfo[p][3]
                '''
    if rootNode == 102:
        print "++++++++++++++++++++++++++++"
        print "nodesCreditContribution"
        print nodesCreditContribution
        print "++++++++++++++++++++++++++++++++++"
        '''
                
                #print "credit of %d = %d" % (p,nodesCreditContribution[p])
            #print "edgeWtDict"
            #print edgeWtDict
    #return edgeWtDict

def orderTuple(a,b):
    if a > b:
        res = (b,a)
    else:
        res = (a,b)
    return res

	
GraphBFS=nx.Graph()
GraphBFS.add_edges_from(listOfEdges)

edgeWtDict = {}
#for i in edgelistTest:
for i in listOfEdges:
    edgeWtDict[i] = 0

#print len(edgeWtDict)

for node in GraphBFS.nodes():
    BFSRoot(node)
	
for i in edgeWtDict.keys():
    val = edgeWtDict[i]
    edgeWtDict[i] =float(val) / float(2)

#f = open("E:/USC/DataMining/Assignment/Assignment4/tmp/Ankitha_Radhakrishna_Betweenness.txt",'w')
f = open("Ankitha_Radhakrishna_Betweenness.txt",'w')
od = collections.OrderedDict(sorted(edgeWtDict.items()))
finalstr = ""
for k, v in od.iteritems(): 
    finalstr += "("+str(k[0])+","+str(k[1])+","+str(v)+")\n"
f.write(finalstr)    
f.close()	

end = time.time()
print "time = "
print end - start

