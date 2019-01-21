package edu.coursera.distributed;

import edu.coursera.distributed.util.MPI;
import edu.coursera.distributed.util.MPI.MPIException;

/**
 * A wrapper class for a parallel, MPI-based matrix multiply implementation.
 */
public class MatrixMult {
    /**
     * A parallel implementation of matrix multiply using MPI to express SPMD
     * parallelism. In particular, this method should store the output of
     * multiplying the matrices firstMatrix and secondMatrix into the matrix resultMatrix.
     * <p>
     * This method is called simultaneously by all MPI ranks in firstMatrix running MPI
     * program. For simplicity MPI_Init has already been called, and
     * MPI_Finalize should not be called in parallelMatrixMultiply.
     * <p>
     * On entry to parallelMatrixMultiply, the following will be true of firstMatrix, secondMatrix,
     * and resultMatrix:
     * <p>
     * 1) The matrix firstMatrix will only be filled with the input values on MPI rank
     * zero. Matrix firstMatrix on all other ranks will be empty (initialized to all
     * zeros).
     * 2) Likewise, the matrix secondMatrix will only be filled with input values on MPI
     * rank zero. Matrix secondMatrix on all other ranks will be empty (initialized to
     * all zeros).
     * 3) Matrix resultMatrix will be initialized to all zeros on all ranks.
     * <p>
     * Upon returning from parallelMatrixMultiply, the following must be true:
     * <p>
     * 1) On rank zero, matrix resultMatrix must be filled with the final output of the
     * full matrix multiplication. The contents of matrix resultMatrix on all other
     * ranks are ignored.
     * <p>
     * Therefore, it is the responsibility of this method to distribute the
     * input data in firstMatrix and secondMatrix across all MPI ranks for maximal parallelism,
     * perform the matrix multiply in parallel, and finally collect the output
     * data in resultMatrix from all ranks back to the zeroth rank. You may use any of the
     * MPI APIs provided in the mpi object to accomplish this.
     * <p>
     * A reference sequential implementation is provided below, demonstrating
     * the use of the Matrix class's APIs.
     *
     * @param firstMatrix  Input matrix
     * @param secondMatrix Input matrix
     * @param resultMatrix Output matrix
     * @param mpi          MPI object supporting MPI APIs
     * @throws MPIException On MPI error. It is not expected that your
     *                      implementation should throw any MPI errors during
     *                      normal operation.
     */
    public static void parallelMatrixMultiply(Matrix firstMatrix, Matrix secondMatrix, Matrix resultMatrix,
                                              final MPI mpi) throws MPIException {

        final int myrank = mpi.MPI_Comm_rank(mpi.MPI_COMM_WORLD);
        final int size = mpi.MPI_Comm_size(mpi.MPI_COMM_WORLD);

        final int nrows = resultMatrix.getNRows();
        final int rowChunk = (nrows + size - 1) / size;
        final int startRow = myrank * rowChunk;
        int endRow = (myrank + 1) * rowChunk;
        if (endRow > nrows) {
            endRow = nrows;
        }

        mpi.MPI_Bcast(firstMatrix.getValues(), 0, firstMatrix.getNRows() * firstMatrix.getNCols(), 0, mpi.MPI_COMM_WORLD);
        mpi.MPI_Bcast(secondMatrix.getValues(), 0, secondMatrix.getNRows() * secondMatrix.getNCols(), 0, mpi.MPI_COMM_WORLD);

        for (int row = startRow; row < endRow; row++) {
            for (int col = 0; col < resultMatrix.getNCols(); col++) {
                resultMatrix.set(row, col, 0.0);
                for (int k = 0; k < secondMatrix.getNRows(); k++) {
                    resultMatrix.incr(row, col, firstMatrix.get(row, k) * secondMatrix.get(k, col));
                }
            }
        }

        if (myrank == 0) {
            MPI.MPI_Request[] requests = new MPI.MPI_Request[size - 1];
            for (int i = 1; i < size; i++) {
                int rankStartRow = i * rowChunk;
                int rankEndRow = (i + 1) * rowChunk;
                if (rankEndRow > nrows) rankEndRow = nrows;

                final int rowOffset = rankStartRow * resultMatrix.getNCols();
                final int nElements = (rankEndRow - rankStartRow) * resultMatrix.getNCols();

                requests[i - 1] = mpi.MPI_Irecv(resultMatrix.getValues(), rowOffset, nElements, i, i, mpi.MPI_COMM_WORLD);
            }
            mpi.MPI_Waitall(requests);

        } else {
            mpi.MPI_Send(resultMatrix.getValues(), startRow * resultMatrix.getNCols(),
                    (endRow - startRow) * resultMatrix.getNCols(), 0, myrank,
                    mpi.MPI_COMM_WORLD);
        }
    }
}
