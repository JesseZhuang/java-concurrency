package edu.coursera.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    private static double seqArraySum(final double[] input, int startIndexInclusive, int endIndexExclusive) {
        double sum = 0;
        for (int i = startIndexInclusive; i < endIndexExclusive; i++) sum += 1 / input[i];
        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks   The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk     The chunk to compute the start of
     * @param nChunks   The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     * nElements
     */
    private static int getChunkStartInclusive(final int chunk,
                                              final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk     The chunk to compute the end of
     * @param nChunks   The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
                                            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {

        private int SEQUENTIAL_THRESHOLD = 1000;
        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;
        //private int numTasks;

        /**
         * Constructor.
         *
         * @param setStartIndexInclusive Set the starting index to begin
         *                               parallel traversal at.
         * @param setEndIndexExclusive   Set ending index for parallel traversal.
         * @param setInput               Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                               final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
            //this.numTasks = numTasks;
        }

        /**
         * Getter for the value produced by this task.
         *
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        public void setSequentialThreshold(int threshold) {
            SEQUENTIAL_THRESHOLD = threshold;
        }

        @Override
        protected void compute() {
            if (endIndexExclusive - startIndexInclusive <= SEQUENTIAL_THRESHOLD) {
                value += seqArraySum(input, startIndexInclusive, endIndexExclusive);
            } else {

// tasks will be increasing by numTasks fold instead of doubling this way, probably not what the question meant
//                ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks];
//                int nElements = endIndexExclusive - startIndexInclusive;
//                for (int i = 0; i < numTasks - 1; i++) {
//                    tasks[i] = new ReciprocalArraySumTask(getChunkStartInclusive(i, numTasks, nElements),
//                            getChunkEndExclusive(i, numTasks, nElements),
//                            input,
//                            numTasks);
//                    System.out.println("numElements " + nElements + " starting index " +
//                            getChunkStartInclusive(i, numTasks, nElements) +
//                            " ending exclusive index " + getChunkEndExclusive(i, numTasks, nElements) + " task " + i);
//                }
//
//                for (int i = 0; i < numTasks - 1; i++) tasks[i].fork();
//
//                tasks[numTasks - 1] = new ReciprocalArraySumTask(getChunkStartInclusive(numTasks - 1, numTasks, nElements),
//                        getChunkEndExclusive(numTasks - 1, numTasks, nElements),
//                        input,
//                        numTasks);
//                tasks[numTasks - 1].compute();
//                for (int i = 0; i < numTasks - 1; i++) tasks[i].join();
//
//                for (int i = 0; i < numTasks; i++) {
//                    value += tasks[i].value;
//                }

                // 2 tasks version
                int mid = startIndexInclusive + (endIndexExclusive - startIndexInclusive) / 2;
                ReciprocalArraySumTask left = new ReciprocalArraySumTask(startIndexInclusive, mid, input);
                ReciprocalArraySumTask right = new ReciprocalArraySumTask(mid, endIndexExclusive, input);
                left.fork();
                right.compute();
                left.join();
                value = left.value + right.value;
            }

        }
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;

        double sum = 0;
        ReciprocalArraySumTask task = new ReciprocalArraySumTask(0, input.length, input);
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(2));
        ForkJoinPool.commonPool().invoke(task);
        sum = task.value;
        return sum;
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input    Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
                                                final int numTasks) {
        //Simply setting java.util.concurrent.ForkJoinPool.common.parallelism to numTasks is not question intention
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(4));
        double sum = 0;
        List<ReciprocalArraySumTask> tasks = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            tasks.add(new ReciprocalArraySumTask(getChunkStartInclusive(i, numTasks, input.length),
                    getChunkEndExclusive(i, numTasks, input.length),
                    input));
        }

        ForkJoinTask.invokeAll(tasks);
        for (int i = 0; i < numTasks; i++) sum += tasks.get(i).value;
        return sum;
    }

    public static void main(String[] args) {
        int arraySize = 1000000;
        double[] array = new double[arraySize];
        for (int i = 0; i < arraySize; i++) array[i] = i + 1;

        for (int i = 0; i < 5; i++) {
            long startTime = System.nanoTime();
            double sum = seqArraySum(array);
            long timeInNanos = System.nanoTime() - startTime;
            System.out.println("sequential sum answer " + sum + " , time " +  timeInNanos / 1000.0 + " ms.");

            startTime = System.nanoTime();
            System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(2));
            sum = parArraySum(array);
            timeInNanos = System.nanoTime() - startTime;
            System.out.println("fork join 2 workers answer " + sum + " , time " + timeInNanos / 1000.0 + " ms.");

            System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(4));
            startTime = System.nanoTime();
            sum = seqArraySum(array);
            timeInNanos = System.nanoTime() - startTime;
            System.out.println("fork join 4 workers answer " + sum + " , time " + timeInNanos / 1000.0 + " ms.");

            startTime = System.nanoTime();
            sum = parManyTaskArraySum(array, 4);
            timeInNanos = System.nanoTime() - startTime;
            System.out.println("parMany(4) workers answer " + sum + " , time " + timeInNanos / 1000.0 + " ms.");
            System.out.println();

        }

    }
}
