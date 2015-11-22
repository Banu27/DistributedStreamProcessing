package edu.uiuc.cs425;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	String path = "/Users/banumuthukumar/Desktop/myfile.jar";
        System.out.println( "Hello World!" );
        ComponentManager myObj = new ComponentManager();
        myObj.ReceiveNewJob("xyz", path);
    }
}
