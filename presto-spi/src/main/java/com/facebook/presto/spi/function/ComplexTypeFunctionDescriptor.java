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

import com.facebook.presto.common.Subfield;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.Subfield.allSubscripts;
import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

/**
 * Contains properties that describe how the function operates on Map or Array inputs.
 */
public class ComplexTypeFunctionDescriptor
{
    public static final List<String> MAP_AND_ARRAY = unmodifiableList(Arrays.asList("map", "array"));
    public static final ComplexTypeFunctionDescriptor DEFAULT = new ComplexTypeFunctionDescriptor(
            true,
            emptyList(),
            Optional.of(emptySet()),
            Optional.of(ComplexTypeFunctionDescriptor::allSubfieldsRequired));

    /**
     * Indicates whether the function accessing subfields.
     */
    private final boolean isAccessingInputValues;

    /**
     * Set of indices of the function arguments containing map or array arguments. Those arguments are important because all accessed subfields collected so far relate only to
     * those map or array arguments and will be passed only to those arguments during the expression analysis phase.
     * If <code>argumentIndicesContainingMapOrArray</code> is <code>Optional.empty()</code>, it indicates that all function arguments are of the map or array types.
     * For example, <code>CONCAT</code> function accepts the variadic argument containing only array arguments.
     * If <code>argumentIndicesContainingMapOrArray</code> is an empty set, it indicates that none of the  function arguments are of the map or array types and thus all the
     * collected subfields will be discarded.
     */
    private final Optional<Set<Integer>> argumentIndicesContainingMapOrArray;

    /**
     * Contains the transformation function to convert the output back to the input elements of the array or map.
     */
    private final Optional<Function<Set<Subfield>, Optional<Set<Subfield>>>> outputToInputTransformationFunction;

    /**
     * Contains the description of all lambdas that this function accepts.
     * If function does not accept any lambda parameter, then <code>lambdaDescriptors</code> should be an empty list.
     */
    private final List<LambdaDescriptor> lambdaDescriptors;

    public ComplexTypeFunctionDescriptor(
            boolean isAccessingInputValues,
            List<LambdaDescriptor> lambdaDescriptors,
            Optional<Set<Integer>> argumentIndicesContainingMapOrArray,
            Optional<Function<Set<Subfield>, Optional<Set<Subfield>>>> outputToInputTransformationFunction,
            Signature signature)
    {
        this(isAccessingInputValues, lambdaDescriptors, argumentIndicesContainingMapOrArray, outputToInputTransformationFunction);
        if (argumentIndicesContainingMapOrArray.isPresent()) {
            checkArgument(argumentIndicesContainingMapOrArray.get().stream().allMatch(index -> index >= 0 &&
                    index < signature.getArgumentTypes().size() &&
                    MAP_AND_ARRAY.contains(signature.getArgumentTypes().get(index).getBase().toLowerCase(Locale.ENGLISH))));
        }
        for (LambdaDescriptor lambdaDescriptor : lambdaDescriptors) {
            checkArgument(lambdaDescriptor.getCallArgumentIndex() >= 0 && signature.getArgumentTypes().get(lambdaDescriptor.getCallArgumentIndex()).isFunction());
            checkArgument(lambdaDescriptor.getLambdaArgumentDescriptors().keySet().stream().allMatch(
                    argumentIndex -> argumentIndex >= 0 && argumentIndex < signature.getArgumentTypes().size()));
            for (Integer lambdaArgumentIndex : lambdaDescriptor.getLambdaArgumentDescriptors().keySet()) {
                checkArgument(lambdaArgumentIndex >= 0 &&
                        lambdaArgumentIndex < signature.getArgumentTypes().get(lambdaDescriptor.getCallArgumentIndex()).getParameters().size() - 1);
                LambdaArgumentDescriptor lambdaArgumentDescriptor = lambdaDescriptor.getLambdaArgumentDescriptors().get(lambdaArgumentIndex);
                checkArgument(lambdaArgumentDescriptor.getCallArgumentIndex() >= 0 &&
                        lambdaArgumentDescriptor.getCallArgumentIndex() < signature.getArgumentTypes().size());
            }
        }
    }

    public ComplexTypeFunctionDescriptor(
            boolean isAccessingInputValues,
            List<LambdaDescriptor> lambdaDescriptors,
            Optional<Set<Integer>> argumentIndicesContainingMapOrArray,
            Optional<Function<Set<Subfield>, Optional<Set<Subfield>>>> outputToInputTransformationFunction)
    {
        requireNonNull(argumentIndicesContainingMapOrArray, "argumentIndicesContainingMapOrArray is null");
        this.isAccessingInputValues = isAccessingInputValues;
        this.lambdaDescriptors = unmodifiableList(requireNonNull(lambdaDescriptors, "lambdaDescriptors is null"));
        this.argumentIndicesContainingMapOrArray = argumentIndicesContainingMapOrArray.isPresent() ?
                Optional.of(unmodifiableSet(argumentIndicesContainingMapOrArray.get())) :
                Optional.empty();
        this.outputToInputTransformationFunction = requireNonNull(outputToInputTransformationFunction, "outputToInputTransformationFunction is null");
    }

    public static ComplexTypeFunctionDescriptor defaultFunctionDescriptor()
    {
        return DEFAULT;
    }

    public boolean isAccessingInputValues()
    {
        return isAccessingInputValues;
    }

    public Optional<Set<Integer>> getArgumentIndicesContainingMapOrArray()
    {
        return argumentIndicesContainingMapOrArray;
    }

    public List<LambdaDescriptor> getLambdaDescriptors()
    {
        return lambdaDescriptors;
    }

    public boolean isAcceptingLambdaArgument()
    {
        return !lambdaDescriptors.isEmpty();
    }

    public Optional<Function<Set<Subfield>, Optional<Set<Subfield>>>> getOutputToInputTransformationFunction()
    {
        return outputToInputTransformationFunction;
    }

    /**
     * Adds <code>allSubscripts</code> on top of the path for every subfield in 'subfields'.
     *
     * @param subfields set of Subfield to transform
     * @return transformed copy of the input set of subfields with <code>allSubscripts</code>.
     */
    public static Optional<Set<Subfield>> prependAllSubscripts(Set<Subfield> subfields)
    {
        return Optional.of(subfields.stream().map(subfield -> new Subfield(subfield.getRootName(),
                        unmodifiableList(
                                Stream.concat(
                                        Arrays.asList(allSubscripts()).stream(),
                                        subfield.getPath().stream()).collect(Collectors.toList()))))
                .collect(Collectors.toSet()));
    }

    /**
     * Transformation function that overrides all lambda subfields from outer functions with the single subfield with <code>allSubscripts</code> in its path.
     * Essentially, it instructs to include all subfields of the array element or map value. This function is most commonly used with the function that
     * returns the entire value from its input or accesses input values internally.
     *
     * @return one subfield with <code>allSubscripts</code> in its path.
     */
    public static Optional<Set<Subfield>> allSubfieldsRequired(Set<Subfield> subfields)
    {
        if (subfields.isEmpty()) {
            return Optional.of(unmodifiableSet(Stream.of(new Subfield("", Arrays.asList(allSubscripts()))).collect(Collectors.toSet())));
        }
        return Optional.of(subfields);
    }

    /**
     * Transformation function that removes any previously accessed subfields. This function is most commonly used with the function that do not return values from its input.
     *
     * @return empty set.
     */
    public static Optional<Set<Subfield>> clearRequiredSubfields(Set<Subfield> ignored)
    {
        return Optional.of(emptySet());
    }
}
