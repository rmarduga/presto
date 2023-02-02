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
package com.facebook.presto.spi.function;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({METHOD, TYPE})
public @interface ScalarFunctionDescriptor
{
    /**
     * Indicates whether the function accessing subfields.
     */
    boolean isAccessingInputValues() default true;

    /**
     * Set of indices of the function arguments containing map or array arguments. Those arguments are important because all accessed subfields collected so far relate only to
     * those map or array arguments and will be passed only to those arguments during the expression analysis phase.
     * If <code>argumentIndicesContainingMapOrArray</code> is <code>Optional.empty()</code>, it indicates that all function arguments are of the map or array types.
     * For example, <code>CONCAT</code> function accepts the variadic argument containing only array arguments.
     * If <code>argumentIndicesContainingMapOrArray</code> is an empty set, it indicates that none of the  function arguments are of the map or array types and thus all the
     * collected subfields will be discarded.
     */
    int[] argumentIndicesContainingMapOrArray() default {};

    /**
     * Contains the transformation function to convert the output back to the input elements of the array or map.
     */
    StaticMethodPointer[] outputToInputTransformationFunction() default {};

    /**
     * Contains the description of all lambdas that this function accepts.
     * If function does not accept any lambda parameter, then <code>lambdaDescriptors</code> should be an empty list.
     */
    ScalarFunctionLambdaDescriptor[] lambdaDescriptors() default {};
}
