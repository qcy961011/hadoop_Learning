package algorithm;

public class PeakIndexinaMountainArray {

    public static void main(String[] args) {
        int[] arr = {18,29,38,59,98,100,99,98,90};
        System.out.println(peakIndexInMountainArray(arr));
    }

    private static int peakIndexInMountainArray(int[] A) {
        int low = 0;
        int high = A.length - 1;
        while (true){
            int res = (low + high) / 2;
            if (res == 0 || res == A.length - 1){
                return -1;
            }
            if (A[res] > A[res - 1] && A[res] > A[res + 1]){
                return res;
            }
            if (A[res + 1] > A[res]) {
                low = res;
                continue;
            }
            if (A[res - 1] > A[res]) {
                high = res;
                continue;
            }
        }
    }

}
