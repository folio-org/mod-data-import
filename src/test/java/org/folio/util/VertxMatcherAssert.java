package org.folio.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

import io.vertx.ext.unit.TestContext;

public class VertxMatcherAssert {

  public static <T> void asyncAssertThat(TestContext context, T actual,
      Matcher<? super T> matcher) {
    asyncAssertThat(context, "", actual, matcher);
  }

  public static <T> void asyncAssertThat(TestContext context, String reason,
      T actual, Matcher<? super T> matcher) {
    if (!matcher.matches(actual)) {
      Description description = new StringDescription();
      description.appendText(reason)
          .appendText("\nExpected: ")
          .appendDescriptionOf(matcher)
          .appendText("\n     but: ");
      matcher.describeMismatch(actual, description);
      context.fail(description.toString());
    }
  }

  public static void asyncAssertThat(TestContext context, String reason,
      boolean assertion) {
    if (!assertion) {
      context.fail(reason);
    }
  }
}
