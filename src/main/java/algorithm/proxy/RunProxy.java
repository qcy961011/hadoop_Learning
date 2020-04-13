package algorithm.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

public class RunProxy {
    public static void main(String[] args) {
        HelloInterface hello = new Hello();
        ByeInterface bye = new Bye();
        InvocationHandler handlerHello = new ProxyHandler(hello);
        InvocationHandler handlerBye = new ProxyHandler(bye);

        HelloInterface proxyHello = (HelloInterface) Proxy.newProxyInstance(hello.getClass().getClassLoader(), hello.getClass().getInterfaces(), handlerHello);
        ByeInterface proxybye = (ByeInterface) Proxy.newProxyInstance(bye.getClass().getClassLoader(), bye.getClass().getInterfaces(), handlerBye);

        proxyHello.sayHello();
        proxybye.sayBye();
    }
}


interface HelloInterface {
    void sayHello();
}

class Hello implements HelloInterface {
    @Override
    public void sayHello() {
        System.out.println("Hello zhanghao!");
    }
}

interface ByeInterface {
    void sayBye();
}

class Bye implements ByeInterface {
    @Override
    public void sayBye() {
        System.out.println("Bye zhanghao!");
    }
}