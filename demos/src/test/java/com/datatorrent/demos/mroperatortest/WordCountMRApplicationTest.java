package com.datatorrent.demos.mroperatortest;

import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.datatorrent.demos.mroperator.NewWordCountApplication;


public class WordCountMRApplicationTest {
	@Test
	public void testSomeMethod() throws Exception {
		LocalMode.runApp(new NewWordCountApplication(), 20000000);
	}

}
