#Required packages
from pyspark import SparkContext
from pyspark.mllib.linalg.distributed import *
import sys

class MatrixArthematic():
    
    def __init__(self):
        sc = SparkContext()

    def add(self, urlA, urlB):
        matrixA = sc.textFile(urlA)
        matrixB = sc.textFile(urlB)
        matIntA = matrixA.map(lambda line: line.split(",")).map(lambda i: [int(j)for j in i])
        matIntB = matrixB.map(lambda line: line.split(",")).map(lambda i: [int(j)for j in i])
        print("Addition of Matrices: \n")
        print(as_block_matrix(matIntA).add(as_block_matrix(matIntB)).toLocalMatrix())
        
        
    def sub(self, urlA, urlB):
        matrixA = sc.textFile(urlA)
        matrixB = sc.textFile(urlB)
        matIntA = matrixA.map(lambda line: line.split(",")).map(lambda i: [int(j)for j in i])
        matIntB = matrixB.map(lambda line: line.split(",")).map(lambda i: [int(j)for j in i])
        print("Subtraction of Matrices: \n")
        print(as_block_matrix(matIntA).subtract(as_block_matrix(matIntB)).toLocalMatrix())
        
    def mul(self, urlA, urlB):
        matrixA = sc.textFile(urlA)
        matrixB = sc.textFile(urlB)
        matIntA = matrixA.map(lambda line: line.split(",")).map(lambda i: [int(j)for j in i])
        matIntB = matrixB.map(lambda line: line.split(",")).map(lambda i: [int(j)for j in i])
        print("Multiplication of Matrices: \n")
        print(as_block_matrix(matIntA).multiply(as_block_matrix(matIntB)).toLocalMatrix())
        
    def as_block_matrix(self, rdd, rowsPerBlock=1024, colsPerBlock=1024):
        return IndexedRowMatrix(rdd.zipWithIndex().map(lambda xi: IndexedRow(xi[1], xi[0]))).toBlockMatrix(rowsPerBlock, colsPerBlock)
    
def main():
    matrixArthematic = MatrixArthematic()
    while True:
        choice = int(input("Choose: \n1.For Addition \n2.For Subtraction \n3.For Multiplication \n4.For Exit \n"))
        if choice == 1:
            urlA = input("Enter matrix A URL:")
            urlB = input("Enter matrix A URL:")
            matrixC = matrixArthematic.add(urlA, urlB)

        elif choice == 2:
            urlA = input("Enter matrix A URL:")
            urlB = input("Enter matrix A URL:")
            matrixC = matrixArthematic.sub(urlA, urlB)
            
        elif choice == 3:
            urlA = input("Enter matrix A URL:")
            urlB = input("Enter matrix A URL:")
            matrixC = matrixArthematic.mul(urlA, urlB)

        elif choice == 4:
            del matrixArthematic
            sc.stop()
            exit(0)

        else:
            print("Enter proper choice: \n")

#Calling main function    
if __name__ == '__main__':
    main()