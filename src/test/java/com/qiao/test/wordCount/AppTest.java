package com.qiao.test.wordCount;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Unit test for simple App.
 */
public class AppTest {

    @Test
    public void index(){
        Text t = new Text("hadoop");
        assertThat(t.getLength() , is(6));
        assertThat(t.getBytes().length , is(6));
        assertThat(t.charAt(2) , is((int)'d'));
    }

}
