package com.han.kafka;

import org.junit.Test;

public class ExceptionTest {
	
	@Test
	public void testException()throws Exception{
		
		testException1();
	}

	
	public void testException1()throws Exception {

		try {
			System.out.println("--1----");

			int a = 1;
			if (a * a == 1) {

				throw new NullPointerException("null");
			}

			System.out.println("--2----");
		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
