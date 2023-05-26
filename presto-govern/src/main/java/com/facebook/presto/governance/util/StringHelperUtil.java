/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.governance.util;

public final class StringHelperUtil
{
    public static final String DOUBLE_QUOTES_SYMBOL = "\"";

    public static final String EMPTY_STRING = "";

    private StringHelperUtil()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * This method will check whether the given input String is NULL/EMPTY
     *
     * @param str
     * @return
     */
    public static boolean isNullString(String str)
    {
        if (str == null || str.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * This method will remove the double quotes from the string if any
     *
     * @param str
     * @return
     */
    public static String removeDoubleQuotes(String str)
    {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.replaceAll(DOUBLE_QUOTES_SYMBOL, EMPTY_STRING);
    }
}
