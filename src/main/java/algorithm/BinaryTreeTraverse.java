package algorithm;

import javafx.util.Pair;

import java.util.*;

public class BinaryTreeTraverse {

    public static void main(String[] args) {
        BinaryTree listnode_2 = new BinaryTree(2);
        BinaryTree listnode_4 = new BinaryTree(4);
        BinaryTree listnode_3 = new BinaryTree(3);
        BinaryTree listnode_5 = new BinaryTree(5);
        BinaryTree listnode_6 = new BinaryTree(6);
        listnode_2.leftNode = listnode_3;
        listnode_2.rightNode = listnode_4;
        listnode_4.leftNode = listnode_5;
        listnode_5.leftNode = listnode_6;
        System.out.println("maxDepth : " + maxDepth(listnode_2));
        System.out.println("minDepthTraverse : " + minDepthTraverse(listnode_2));
        System.out.println("minDepthDFS : " + minDepthDFS(listnode_2));
        System.out.println("minDepthBFS : " + minDepthBFS(listnode_2));
        System.out.println("maxWidthBFS : " + maxWidthBFS(listnode_2));
        System.out.println("maxWidthDFS : " + maxWidthDFS(listnode_2));
    }

    public static int maxDepth(BinaryTree root) {
        return root == null ? 0 : 1 + Math.max(maxDepth(root.leftNode), maxDepth(root.rightNode));
    }

    public static int minDepthTraverse(BinaryTree root) {
        if (root == null) {
            return 0;
        }
        if (root.leftNode == null && root.rightNode == null) {
            return 1;
        }
        int min_depth = Integer.MAX_VALUE;
        if (root.leftNode != null) {
            min_depth = Math.min(minDepthTraverse(root.leftNode), min_depth);
        }
        if (root.rightNode != null) {
            min_depth = Math.min(minDepthTraverse(root.rightNode), min_depth);
        }
        return min_depth + 1;
    }

    public static int minDepthDFS(BinaryTree root) {
        Stack<Pair<BinaryTree, Integer>> stack = new Stack<>();
        if (root == null) {
            return 0;
        } else {
            stack.push(new Pair<>(root, 1));
        }
        int minDepth = Integer.MAX_VALUE;
        while (!stack.isEmpty()) {
            Pair<BinaryTree, Integer> pop = stack.pop();
            root = pop.getKey();
            int current = pop.getValue();
            if (root.leftNode == null && root.rightNode == null) {
                minDepth = Math.min(current, minDepth);
            }
            if (root.leftNode != null) {
                stack.push(new Pair<>(root.leftNode, current + 1));
            }
            if (root.rightNode != null) {
                stack.push(new Pair<>(root.rightNode, current + 1));
            }
        }
        return minDepth;
    }

    public static int minDepthBFS(BinaryTree root) {
        LinkedList<Pair<BinaryTree, Integer>> stack = new LinkedList<>();
        if (root == null) {
            return 0;
        } else {
            stack.add(new Pair<>(root, 1));
        }
        int minDepth = Integer.MAX_VALUE;
        while (!stack.isEmpty()) {
            Pair<BinaryTree, Integer> pop = stack.poll();
            root = pop.getKey();
            minDepth = pop.getValue();
            if (root.leftNode == null && root.rightNode == null) {
                break;
            }
            if (root.leftNode != null) {
                stack.add(new Pair<>(root.leftNode, minDepth + 1));
            }
            if (root.rightNode != null) {
                stack.add(new Pair<>(root.rightNode, minDepth + 1));
            }
        }
        return minDepth;
    }

    public static int maxWidthBFS(BinaryTree root) {
        Queue<AnnotatedNode> queue = new LinkedList();
        queue.add(new AnnotatedNode(root, 0, 0));
        int curDepth = 0;
        int left = 0;
        int ans = 0;
        while (!queue.isEmpty()) {
            AnnotatedNode node = queue.poll();
            if (node.node != null) {
                queue.add(new AnnotatedNode(node.node.leftNode, node.depth + 1, node.pos * 2));
                queue.add(new AnnotatedNode(node.node.rightNode, node.depth + 1, node.pos * 2 + 1));
                if (curDepth != node.depth) {
                    curDepth = node.depth;
                    left = node.pos;
                }
                ans = Math.max(ans, node.pos - left + 1);
            }
        }
        return ans;
    }

    private static int ans = 0;
    private static Map<Integer, Integer> left = new HashMap<>();

    public static int maxWidthDFS(BinaryTree root) {
        dfs(root, 0, 0);
        return ans;
    }

    private static void dfs(BinaryTree root, int depth, int pos) {
        if (root == null) {
            return;
        }
        left.computeIfAbsent(depth, x -> pos);
        ans = Math.max(ans, pos - left.get(depth) + 1);
        dfs(root.leftNode, depth + 1, 2 * pos);
        dfs(root.rightNode, depth + 1, 2 * pos + 1);
    }
}


class BinaryTree {
    BinaryTree leftNode;
    BinaryTree rightNode;
    int value;

    public BinaryTree(int value) {
        this.value = value;
    }
}


class AnnotatedNode {
    BinaryTree node;
    int depth, pos;

    AnnotatedNode(BinaryTree n, int d, int p) {
        node = n;
        depth = d;
        pos = p;
    }
}
