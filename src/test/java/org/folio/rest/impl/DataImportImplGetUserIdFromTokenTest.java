package org.folio.rest.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DataImportImplGetUserIdFromTokenTest {

  private static String base64Encode(String plaintext) {
    return Base64.getEncoder().encodeToString(plaintext.getBytes());
  }

  @Parameters
  public static Collection<Object[]> getExpectedValues() {
    return Arrays.asList(
      new Object[] { null, null },
      new Object[] { "foo", null },
      new Object[] { "jwt.invalid", null },
      new Object[] { "jwt." + base64Encode("invalid"), null },
      new Object[] { "jwt." + base64Encode("{}"), null },
      new Object[] { "jwt." + base64Encode("{\"user_id\":\"foo\"}"), "foo" }
    );
  }

  private String token;
  private String expected;

  public DataImportImplGetUserIdFromTokenTest(String token, String expected) {
    this.token = token;
    this.expected = expected;
  }

  @Test
  public void testGetUserIdFromToken() {
    assertThat(DataImportImpl.getUserIdFromToken(token), is(expected));
  }
}
