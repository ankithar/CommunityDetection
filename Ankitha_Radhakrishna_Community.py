
#import findspark
#findspark.init()

import os, sys
from pyspark import SparkContext
import time
import math
import Queue
import collections
from collections import OrderedDict
import operator

import networkx as nx
from networkx import connected_component_subgraphs
from networkx import number_connected_components
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
        
#mid1 = time.time()
#print "mid1"
#print mid1 - start

def BFSRoot(rootNode):
    #print "BFSRoot for %d " % rootNode
    BFSInfo = {}
    for node in GraphBFS.nodes:
        BFSInfo[node] = [0,0,[],0] #[visited,level,listofparents,numberOfPaths]
    BFSQueue = Queue.Queue()
    BFSStack = Queue.LifoQueue()
    nodesCreditContribution = {}
    BFSQueue.put(rootNode)
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
'''
f = open("E:/USC/DataMining/Assignment/Assignment4/tmp/Ankitha_Radhakrishna_Betweenness_MapPart2.txt",'w')
#f = open("Ankitha_Radhakrishna_Betweenness.txt",'w')
od = collections.OrderedDict(sorted(edgeWtDict.items()))
finalstr = ""
for k, v in od.iteritems(): 
    finalstr += "("+str(k[0])+","+str(k[1])+","+str(v)+")\n"
f.write(finalstr)    
f.close()
'''

#end = time.time()
#print "time = "
#print end - start


adjMatrix = {}
for i in range(1,671):
    for j in range(i+1,672):
        if GraphBFS.has_edge(i,j):
            adjMatrix[(i,j)] = 1
        else:
            adjMatrix[(i,j)] = 0


orderDict = {}
for i in range(1,672):
    orderDict[i] = len(GraphBFS.edges(i))
#print len(orderDict)

sortedEdges = sorted(edgeWtDict, reverse = True, key=edgeWtDict.__getitem__)

adjacencyDict = dict((el,1) for el in listOfEdges)
#print len(adjacencyDict.keys())

GraphTest1=nx.Graph()
GraphTest1.add_edges_from(listOfEdges)

m = GraphTest1.number_of_edges()


def checkMod(G):
    c = nx.connected_component_subgraphs(G)
    lsTest = list(c)
    '''
    print "len of connected components"
    print len(lsTest)
    print lsTest
    '''
    listOfComm = []
    sum2 = 0.0
    for community in lsTest:
        commNodeList = list(community.nodes())
        #print "no of nodes"
        #print len(commNodeList)
        listOfComm.append(commNodeList)
        length = len(commNodeList)
        #print "comm"
        #print commNodeList
        #print "commNodeList"
        #print commNodeList
        
        for i in range(length-1):
            for j in range(i+1,length):
                #print i,j
                k1 = commNodeList[i]
                k2 = commNodeList[j]
               
                #print "tuple"
                t1 = orderTuple(k1,k2)
                
#                 if edgeWtDict.get;(t1):
                #if t1 in edgeWtDict:
                if adjMatrix[t1] == 1:
#                     print "isnide"
                    A = 1
                else:
#                     print "A = 0"
                    A = 0
                
                ##print A
#                 print "========="
                k1Degree = orderDict[k1]
                k2Degree = orderDict[k2]
                
                term2 = float(k1Degree * k2Degree)/(2 * m)
    
                term1 = A - term2
                '''
                if k1 == 1 and k2 == 2:
                    print "k1Degree"
                    print k1Degree
                    print "k2Degree"
                    print k2Degree
                    print "term2"
                    print term2
                    print "term1 = "
                    print term1
                    print "m"
                    print m
                    print "====="
                '''
                sum2 += term1 
    mod = sum2/float(2 * m)
    #print "mod"
    #print mod
    #print "returning from checkMod"
    return mod,listOfComm
'''
s = time.time()
numberOfConnectedComponents = 1
listOfCommunities = []
highestMod = 0
for edge in sortedEdges:
    
    GraphTest1.remove_edge(*edge)
    #sortedEdges.pop(0)
    num = number_connected_components(GraphTest1)
    if num > numberOfConnectedComponents:
        #print "highestMod"
        #print highestMod
        print "edge = "
        print edge
        numberOfConnectedComponents = num
        mod,ls = checkMod(GraphTest1)
        #print "mod"
        #print mod
        if mod > highestMod:
            highestMod = mod
            listOfCommunities = ls
            
print "*************************"
print highestMod
print listOfCommunities
print "time"
e = time.time()
print e - s

'''


sorted_d2 = sorted(edgeWtDict.items(), key=operator.itemgetter(1), reverse = True)
#print len(sorted_d2)

sorted_d3 = OrderedDict(sorted(edgeWtDict.items(), key=operator.itemgetter(1), reverse = True))
#print len(sorted_d3)


RDD1 = sc.parallelize(sorted_d2)
#print RDD1.count()


'''
def checkMod2(iterator):
    #def _phase2(iterable):
    
    #LS = list()
    dict1 = {}
    for x in iterator:
        #LS.append(x)
        dict1[x[0]] = x[1]
    sorted_d2 = OrderedDict(sorted(dict1.items(), key=operator.itemgetter(1), reverse = True))
    LS1 = list(sorted_d2.keys())
    LS2 = list(sorted_d2.values())
    print LS1[0]
    print LS2[0]
    print LS1[len(LS1) - 1]
    print LS2[len(LS2) - 1]
    print len(LS1)
    print "========"
    return [len(LS1)]
        #print x
    #print "============"
    #return [len(LS)]
   # return _phase2


'''


def checkMod3(G,sorted_d3):
    def phase2(iterable):
        dict1 = {}
        for x in iterable:
            #LS.append(x)
            dict1[x[0]] = x[1]
        sorted_d2 = OrderedDict(sorted(dict1.items(), key=operator.itemgetter(1), reverse = True))
        LS1 = list(sorted_d2.keys())
        #print LS1[0]
        LSKeys = sorted_d3.keys()
        for i in LSKeys:
            if i == LS1[0]:
                break;
            else:
                G.remove_edge(*i)
                
                
        #numberOfConnectedComponents = 1
        numberOfConnectedComponents = number_connected_components(G)
        listOfCommunities = []
        highestMod = 0
        for edge in LS1:
            
            G.remove_edge(*edge)
            #sortedEdges.pop(0)
            num = number_connected_components(G)
            if num > numberOfConnectedComponents:
                #print "highestMod"
                #print highestMod
                #print "edge = "
                #print edge
                numberOfConnectedComponents = num
                mod,ls = checkMod(G)
                #print "mod"
                #print mod
                if mod > highestMod:
                    highestMod = mod
                    listOfCommunities = ls
                    
        return [[highestMod,listOfCommunities]]
    return phase2
    
    

st = time.time()
GraphTest1=nx.Graph()
GraphTest1.add_edges_from(listOfEdges)
res = RDD1.mapPartitions(checkMod3(GraphTest1,sorted_d3))
val = res.collect()


maxIndex = 0
for i in range(1,len(val)):
    if val[i][0] > val[maxIndex][0]:
        maxIndex = i
#print i

print "Highest Modularity = "
print val[i][0]

listOfCommunities = val[i][1]
for i in range(len(listOfCommunities)):
	listOfCommunities[i] = sorted(listOfCommunities[i])
listOfCommunities = sorted(listOfCommunities)
#print listOfCommunities

f2 = open("Ankitha_Radhakrishna_Community.txt",'w')
#f2 = open("E:/USC/DataMining/Assignment/Assignment4/tmp/Ankitha_Radhakrishna_Community_MapPart2.txt",'w')
strComm = ""
for j in listOfCommunities:
	strComm += str(j)+"\n"
f2.write(strComm)
f2.close()

en = time.time()
#print "time = "
#print en - st

print "final time = "
print en - start

