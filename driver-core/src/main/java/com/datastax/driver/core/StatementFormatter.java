/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A customizable component to format instances of {@link Statement}.
 * <p/>
 * Instances of {@link StatementFormatter} can be obtained
 * through the {@link #builder()} method.
 * <p/>
 * Instances of this class are thread-safe.
 */
public final class StatementFormatter {

    public static final StatementFormatter DEFAULT_INSTANCE = StatementFormatter.builder().build();

    // limits

    public static final int DEFAULT_MAX_QUERY_STRING_LENGTH = 500;
    public static final int DEFAULT_MAX_BOUND_VALUE_LENGTH = 50;
    public static final int DEFAULT_MAX_BOUND_VALUES = 50;
    public static final int DEFAULT_MAX_INNER_STATEMENTS = 5;

    // symbols

    public static final String DEFAULT_SUMMARY_START = " [";
    public static final String DEFAULT_SUMMARY_END = "]";
    public static final String DEFAULT_BOUND_VALUES_COUNT = "%s bound values";
    public static final String DEFAULT_INNER_STATEMENTS_COUNT = "%s inner statements";
    public static final String DEFAULT_QUERY_STRING_START = ": ";
    public static final String DEFAULT_QUERY_STRING_END = " ";
    public static final String DEFAULT_BOUND_VALUES_START = "{ ";
    public static final String DEFAULT_BOUND_VALUES_END = " }";
    public static final String DEFAULT_TRUNCATED_OUTPUT_MESSAGE = "<TRUNCATED>";
    public static final String DEFAULT_FURTHER_BOUND_VALUES_OMITTED = "<OTHER VALUES OMITTED>";
    public static final String DEFAULT_FURTHER_INNER_STATEMENTS_OMITTED = "<OTHER STATEMENTS OMITTED>";
    public static final String DEFAULT_NULL_BOUND_VALUE = "<NULL>";
    public static final String DEFAULT_UNSET_BOUND_VALUE = "<UNSET>";
    public static final String DEFAULT_LIST_ELEMENT_SEPARATOR = ", ";
    public static final String DEFAULT_NAME_VALUE_SEPARATOR = " : ";

    private static final int UNLIMITED = -1;
    private static final int MAX_EXCEEDED = -2;

    /**
     * Creates a new {@link StatementFormatter.Builder} instance.
     *
     * @return the new StatementFormatter builder.
     */
    public static StatementFormatter.Builder builder() {
        return new StatementFormatter.Builder();
    }

    /**
     * The desired statement format verbosity.
     * <p/>
     * This should be used as a guideline as to how much information
     * about the statement should be extracted and formatted.
     */
    public enum StatementFormatVerbosity {

        // the enum order matters

        /**
         * Formatters should only print a basic information in summarized form.
         */
        ABRIDGED,

        /**
         * Formatters should print basic information in summarized form,
         * and the statement's query string, if available.
         * <p/>
         * For batch statements, this verbosity level should
         * allow formatters to print information about the batch's
         * inner statements.
         */
        NORMAL,

        /**
         * Formatters should print full information, including
         * the statement's query string, if available,
         * and the statement's bound values, if available.
         */
        EXTENDED

    }

    /**
     * A statement printer is responsible for printing a specific type of {@link Statement statement},
     * with a given {@link StatementFormatVerbosity verbosity level},
     * and using a given {@link StatementWriter statement writer}.
     *
     * @param <S> The type of statement that this printer handles
     */
    public interface StatementPrinter<S extends Statement> {

        /**
         * The concrete {@link Statement} subclass that this printer handles.
         * <p/>
         * In case of subtype polymorphism, if this printer
         * handles more than one concrete subclass,
         * the most specific common ancestor should be returned here.
         *
         * @return The concrete {@link Statement} subclass that this printer handles.
         */
        Class<S> getSupportedStatementClass();

        /**
         * Prints the given {@link Statement statement},
         * using the given {@link StatementWriter statement writer} and
         * the given {@link StatementFormatVerbosity verbosity level}.
         *
         * @param statement the statement to print
         * @param out       the writer to use
         * @param verbosity the verbosity to use
         */
        void print(S statement, StatementWriter out, StatementFormatVerbosity verbosity);

    }

    /**
     * Thrown when a {@link StatementFormatter} encounters an error
     * while formatting a {@link Statement}.
     */
    public static class StatementFormatException extends RuntimeException {

        private final Statement statement;
        private final StatementFormatVerbosity verbosity;
        private final ProtocolVersion protocolVersion;
        private final CodecRegistry codecRegistry;

        public StatementFormatException(Statement statement, StatementFormatVerbosity verbosity, ProtocolVersion protocolVersion, CodecRegistry codecRegistry, Throwable t) {
            super(t);
            this.statement = statement;
            this.verbosity = verbosity;
            this.protocolVersion = protocolVersion;
            this.codecRegistry = codecRegistry;
        }

        public Statement getStatement() {
            return statement;
        }

        public StatementFormatVerbosity getVerbosity() {
            return verbosity;
        }

        public ProtocolVersion getProtocolVersion() {
            return protocolVersion;
        }

        public CodecRegistry getCodecRegistry() {
            return codecRegistry;
        }
    }

    public static final class StatementWriterSymbols {

        public final String summaryStart;
        public final String summaryEnd;
        public final String boundValuesCount;
        public final String statementsCount;
        public final String queryStringStart;
        public final String queryStringEnd;
        public final String boundValuesStart;
        public final String boundValuesEnd;
        public final String truncatedOutput;
        public final String furtherBoundValuesOmitted;
        public final String furtherInnerStatementsOmitted;
        public final String nullBoundValue;
        public final String unsetBoundValue;
        public final String listElementSeparator;
        public final String nameValueSeparator;

        private StatementWriterSymbols(
                // summary
                String summaryStart, String summaryEnd, String boundValuesCount, String statementsCount,
                // query string
                String queryStringStart, String queryStringEnd,
                // bound values
                String boundValuesStart, String boundValuesEnd,
                // misc
                String truncatedOutput, String furtherBoundValuesOmitted, String furtherInnerStatementsOmitted,
                String nullBoundValue, String unsetBoundValue,
                String listElementSeparator, String nameValueSeparator
        ) {
            this.summaryStart = summaryStart;
            this.boundValuesCount = boundValuesCount;
            this.statementsCount = statementsCount;
            this.summaryEnd = summaryEnd;
            this.queryStringStart = queryStringStart;
            this.queryStringEnd = queryStringEnd;
            this.boundValuesStart = boundValuesStart;
            this.boundValuesEnd = boundValuesEnd;
            this.truncatedOutput = truncatedOutput;
            this.furtherBoundValuesOmitted = furtherBoundValuesOmitted;
            this.furtherInnerStatementsOmitted = furtherInnerStatementsOmitted;
            this.nullBoundValue = nullBoundValue;
            this.unsetBoundValue = unsetBoundValue;
            this.listElementSeparator = listElementSeparator;
            this.nameValueSeparator = nameValueSeparator;
        }

    }

    /**
     * A set of user-defined limitation rules that formatters
     * should strive to comply with when formatting statements.
     * <p/>
     * This class is comprised only of public, final fields.
     */
    public static final class StatementWriterLimits {

        /**
         * The maximum length allowed for query strings.
         * <p/>
         * If the query string length exceeds this threshold,
         * formatters should append the
         * {@link StatementWriterSymbols#truncatedOutput} symbol.
         */
        public final int maxQueryStringLength;

        /**
         * The maximum length, in numbers of printed characters,
         * allowed for a single bound value.
         * <p/>
         * If the bound value length exceeds this threshold,
         * formatters should append the
         * {@link StatementWriterSymbols#truncatedOutput} symbol.
         */
        public final int maxBoundValueLength;

        /**
         * The maximum number of printed bound values.
         * <p/>
         * If the number of bound values exceeds this threshold,
         * formatters should append the
         * {@link StatementWriterSymbols#furtherBoundValuesOmitted} symbol.
         */
        public final int maxBoundValues;

        /**
         * The maximum number of printed inner statements
         * of a {@link BatchStatement}.
         * <p/>
         * If the number of inner statements exceeds this threshold,
         * formatters should append the
         * {@link StatementWriterSymbols#furtherInnerStatementsOmitted} symbol.
         * <p/>
         * If the statement to format is not a batch statement,
         * then this setting should be ignored.
         */
        public final int maxInnerStatements;

        private StatementWriterLimits(int maxQueryStringLength, int maxBoundValueLength, int maxBoundValues, int maxInnerStatements) {
            this.maxQueryStringLength = maxQueryStringLength;
            this.maxBoundValueLength = maxBoundValueLength;
            this.maxBoundValues = maxBoundValues;
            this.maxInnerStatements = maxInnerStatements;
        }

    }

    /**
     * A registry for {@link StatementPrinter statement printers}.
     * <p/>
     * This class is thread-safe.
     */
    public static final class StatementPrinterRegistry {

        private final LoadingCache<Class<? extends Statement>, StatementPrinter> printers;

        private StatementPrinterRegistry() {
            printers = CacheBuilder.newBuilder().build(new CacheLoader<Class<? extends Statement>, StatementPrinter>() {
                @SuppressWarnings({"raw", "unchecked"})
                @Override
                public StatementPrinter load(Class key) throws Exception {
                    StatementPrinter printer = null;
                    while (printer == null) {
                        key = key.getSuperclass();
                        printer = printers.get(key);
                    }
                    return printer;
                }
            });

        }

        /**
         * Attempts to locate the best {@link StatementPrinter printer} for the given
         * statement.
         *
         * @param statement The statement find a printer for.
         * @return The best {@link StatementPrinter printer} for the given
         * statement. Cannot be {@code null}.
         */
        @SuppressWarnings("unchecked")
        public <S extends Statement> StatementPrinter<? super S> findPrinter(S statement) {
            try {
                return printers.get(statement.getClass());
            } catch (ExecutionException e) {
                // will never happen as long as a default statement printer is registered
                throw Throwables.propagate(e);
            }
        }

        private <S extends Statement> void register(StatementPrinter<S> printer) {
            printers.put(printer.getSupportedStatementClass(), printer);
        }

    }

    /**
     * A statement writer exposes utility methods to help
     * {@link StatementPrinter statement printers} in formatting
     * a statement.
     * <p/>
     * Direct access to the underlying {@link StringBuilder buffer}
     * is also possible.
     * <p/>
     * This class is NOT thread-safe.
     */
    public static final class StatementWriter {

        private final StringBuilder buffer;
        private final StatementWriterLimits limits;
        private final StatementPrinterRegistry printerRegistry;
        private final StatementWriterSymbols symbols;
        private final ProtocolVersion protocolVersion;
        private final CodecRegistry codecRegistry;
        private int remainingQueryStringChars;
        private int remainingBoundValues;

        private StatementWriter(
                StatementPrinterRegistry printerRegistry, StatementWriterLimits limits, StatementWriterSymbols symbols,
                ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
            this.buffer = new StringBuilder();
            this.printerRegistry = printerRegistry;
            this.limits = limits;
            this.symbols = symbols;
            this.protocolVersion = protocolVersion;
            this.codecRegistry = codecRegistry;
            remainingQueryStringChars = limits.maxQueryStringLength == UNLIMITED ? Integer.MAX_VALUE : limits.maxQueryStringLength;
            remainingBoundValues = limits.maxBoundValues == UNLIMITED ? Integer.MAX_VALUE : limits.maxBoundValues;
        }

        public StatementPrinterRegistry getPrinterRegistry() {
            return printerRegistry;
        }

        public StatementWriterLimits getLimits() {
            return limits;
        }

        public StatementWriterSymbols getSymbols() {
            return symbols;
        }

        public ProtocolVersion getProtocolVersion() {
            return protocolVersion;
        }

        public CodecRegistry getCodecRegistry() {
            return codecRegistry;
        }

        public StringBuilder getBuffer() {
            return buffer;
        }

        public int getRemainingQueryStringChars() {
            return remainingQueryStringChars;
        }

        public int getRemainingBoundValues() {
            return remainingBoundValues;
        }

        public void resetRemainingQueryStringChars() {
            remainingQueryStringChars = limits.maxQueryStringLength == UNLIMITED ? Integer.MAX_VALUE : limits.maxQueryStringLength;
        }

        public void resetRemainingBoundValues() {
            remainingBoundValues = limits.maxBoundValues == UNLIMITED ? Integer.MAX_VALUE : limits.maxBoundValues;
        }

        public boolean maxQueryStringLengthExceeded() {
            return remainingQueryStringChars == MAX_EXCEEDED;
        }

        public boolean maxAppendedBoundValuesExceeded() {
            return remainingBoundValues == MAX_EXCEEDED;
        }

        public StatementWriter appendSummaryStart() {
            buffer.append(symbols.summaryStart);
            return this;
        }

        public StatementWriter appendSummaryEnd() {
            buffer.append(symbols.summaryEnd);
            return this;
        }

        public StatementWriter appendQueryStringStart() {
            buffer.append(symbols.queryStringStart);
            return this;
        }

        public StatementWriter appendQueryStringEnd() {
            buffer.append(symbols.queryStringEnd);
            return this;
        }

        public StatementWriter appendBoundValuesStart() {
            buffer.append(symbols.boundValuesStart);
            return this;
        }

        public StatementWriter appendBoundValuesEnd() {
            buffer.append(symbols.boundValuesEnd);
            return this;
        }

        public StatementWriter appendBatchType(BatchStatement.Type batchType) {
            buffer.append(batchType);
            return this;
        }

        public StatementWriter appendTruncatedOutput() {
            buffer.append(symbols.truncatedOutput);
            return this;
        }

        public StatementWriter appendFurtherBoundValuesOmitted() {
            buffer.append(symbols.furtherBoundValuesOmitted);
            return this;
        }

        public StatementWriter appendFurtherInnerStatementsOmitted() {
            buffer.append(symbols.furtherInnerStatementsOmitted);
            return this;
        }

        public StatementWriter appendNull() {
            buffer.append(symbols.nullBoundValue);
            return this;
        }

        public StatementWriter appendUnset() {
            buffer.append(symbols.unsetBoundValue);
            return this;
        }

        public StatementWriter appendListElementSeparator() {
            buffer.append(symbols.listElementSeparator);
            return this;
        }

        public StatementWriter appendNameValueSeparator() {
            buffer.append(symbols.nameValueSeparator);
            return this;
        }

        public StatementWriter appendClassNameAndHashCode(Statement statement) {
            buffer.append(statement.getClass().getName());
            buffer.append('@');
            buffer.append(Integer.toHexString(statement.hashCode()));
            return this;
        }

        public StatementWriter appendBoundValuesCount(int totalBoundValuesCount) {
            buffer.append(String.format(symbols.boundValuesCount, totalBoundValuesCount));
            return this;
        }

        public StatementWriter appendInnerStatementsCount(int numberOfInnerStatements) {
            buffer.append(String.format(symbols.statementsCount, numberOfInnerStatements));
            return this;
        }

        /**
         * Appends the given fragment as a query string fragment.
         * <p>
         * This method can be called multiple times, in case the formatter
         * needs to compute the query string by pieces.
         * <p>
         * This methods also keeps track of the amount of characters used so far
         * to print the query string, and automatically detects when
         * the query string exceeds {@link StatementWriterLimits#maxQueryStringLength the maximum length},
         * in which case it appends only once the {@link StatementWriterSymbols#truncatedOutput truncated output} symbol.
         *
         * @param queryStringFragment The query string fragment to append
         * @return this writer (for method chaining).
         */
        public StatementWriter appendQueryStringFragment(String queryStringFragment) {
            if (maxQueryStringLengthExceeded())
                return this;
            else if (queryStringFragment.isEmpty())
                return this;
            else if (limits.maxQueryStringLength == UNLIMITED)
                buffer.append(queryStringFragment);
            else if (queryStringFragment.length() > remainingQueryStringChars) {
                if (remainingQueryStringChars > 0) {
                    queryStringFragment = queryStringFragment.substring(0, remainingQueryStringChars);
                    buffer.append(queryStringFragment);
                }
                appendTruncatedOutput();
                remainingQueryStringChars = MAX_EXCEEDED;
            } else {
                buffer.append(queryStringFragment);
                remainingQueryStringChars -= queryStringFragment.length();
            }
            return this;
        }

        public StatementWriter appendBoundValue(int index, ByteBuffer serialized, DataType type) {
            if (maxAppendedBoundValuesExceeded())
                return this;
            return appendBoundValue(Integer.toString(index), serialized, type);
        }

        public StatementWriter appendBoundValue(String name, ByteBuffer serialized, DataType type) {
            if (maxAppendedBoundValuesExceeded())
                return this;
            TypeCodec<Object> codec = codecRegistry.codecFor(type);
            Object value = codec.deserialize(serialized, protocolVersion);
            return appendBoundValue(name, value, type);
        }

        public StatementWriter appendBoundValue(int index, Object value, DataType type) {
            if (maxAppendedBoundValuesExceeded())
                return this;
            return appendBoundValue(Integer.toString(index), value, type);
        }

        public StatementWriter appendBoundValue(String name, Object value, DataType type) {
            if (maxAppendedBoundValuesExceeded())
                return this;
            if (value == null) {
                doAppendBoundValue(name, symbols.nullBoundValue);
                return this;
            } else if (value instanceof ByteBuffer && limits.maxBoundValueLength != UNLIMITED) {
                ByteBuffer byteBuffer = (ByteBuffer) value;
                int maxBufferLengthInBytes = Math.max(2, limits.maxBoundValueLength / 2) - 1;
                boolean bufferLengthExceeded = byteBuffer.remaining() > maxBufferLengthInBytes;
                // prevent large blobs from being converted to strings
                if (bufferLengthExceeded) {
                    byteBuffer = (ByteBuffer) byteBuffer.duplicate().limit(maxBufferLengthInBytes);
                    // force usage of blob codec as any other codec would probably fail to format
                    // a cropped byte buffer anyway
                    String formatted = TypeCodec.blob().format(byteBuffer);
                    doAppendBoundValue(name, formatted);
                    appendTruncatedOutput();
                    return this;
                }
            }
            TypeCodec<Object> codec = type == null ? codecRegistry.codecFor(value) : codecRegistry.codecFor(type, value);
            doAppendBoundValue(name, codec.format(value));
            return this;
        }

        public StatementWriter appendUnsetBoundValue(String name) {
            doAppendBoundValue(name, symbols.unsetBoundValue);
            return this;
        }

        private void doAppendBoundValue(String name, String value) {
            if (maxAppendedBoundValuesExceeded())
                return;
            if (remainingBoundValues == 0) {
                appendFurtherBoundValuesOmitted();
                remainingBoundValues = MAX_EXCEEDED;
                return;
            }
            boolean lengthExceeded = false;
            if (limits.maxBoundValueLength != UNLIMITED && value.length() > limits.maxBoundValueLength) {
                value = value.substring(0, limits.maxBoundValueLength);
                lengthExceeded = true;
            }
            if (name != null) {
                buffer.append(name);
                appendNameValueSeparator();
            }
            buffer.append(value);
            if (lengthExceeded)
                appendTruncatedOutput();
            if (limits.maxBoundValues != UNLIMITED)
                remainingBoundValues--;
        }

        @Override
        public String toString() {
            return buffer.toString();
        }
    }

    /**
     * A common parent class for {@link StatementPrinter} implementations.
     * <p/>
     * This class assumes a common formatting pattern comprised of the following
     * sections:
     * <ol>
     * <li>Header: this section should contain only basic and/or summarized information about the statement;
     * typical examples of information that could be included here are: the actual statement class;
     * the statement's hash code; the number of bound values; etc.</li>
     * <li>Query String: this section should print the statement's query string, if it is available;
     * this section is only enabled if the verbosity is {@link StatementFormatVerbosity#NORMAL NORMAL} or higher;</li>
     * <li>Bound Values: this section should print the statement's bound values, if available;
     * this section is only enabled if the verbosity is {@link StatementFormatVerbosity#EXTENDED EXTENDED};</li>
     * <li>Footer: an optional section, empty by default.</li>
     * </ol>
     */
    public static class StatementPrinterBase<S extends Statement> implements StatementPrinter<S> {

        private final Class<S> supportedStatementClass;

        protected StatementPrinterBase(Class<S> supportedStatementClass) {
            this.supportedStatementClass = supportedStatementClass;
        }

        @Override
        public Class<S> getSupportedStatementClass() {
            return supportedStatementClass;
        }

        @Override
        public void print(S statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            printHeader(statement, out);
            if (verbosity.ordinal() >= StatementFormatVerbosity.NORMAL.ordinal()) {
                printQueryString(statement, out);
                if (verbosity.ordinal() >= StatementFormatVerbosity.EXTENDED.ordinal()) {
                    printBoundValues(statement, out);
                }
            }
            printFooter(statement, out);
        }

        protected void printHeader(S statement, StatementWriter out) {
            out.appendClassNameAndHashCode(statement);
        }

        protected void printQueryString(S statement, StatementWriter out) {
        }

        protected void printBoundValues(S statement, StatementWriter out) {
        }

        protected void printFooter(S statement, StatementWriter out) {
        }

    }

    /**
     * Common parent class for {@link StatementPrinter} implementations dealing with subclasses of {@link RegularStatement}.
     */
    public abstract static class AbstractRegularStatementPrinter<S extends RegularStatement> extends StatementPrinterBase<S> {

        protected AbstractRegularStatementPrinter(Class<S> supportedStatementClass) {
            super(supportedStatementClass);
        }

        @Override
        protected void printQueryString(S statement, StatementWriter out) {
            out.resetRemainingQueryStringChars();
            out.appendQueryStringStart();
            out.appendQueryStringFragment(statement.getQueryString(out.getCodecRegistry()));
            out.appendQueryStringEnd();
        }

    }

    public static class SimpleStatementPrinter extends AbstractRegularStatementPrinter<SimpleStatement> {

        public SimpleStatementPrinter() {
            super(SimpleStatement.class);
        }

        @Override
        protected void printHeader(SimpleStatement statement, StatementWriter out) {
            super.printHeader(statement, out);
            out.appendSummaryStart();
            out.appendBoundValuesCount(statement.valuesCount());
            out.appendSummaryEnd();
        }

        @Override
        protected void printBoundValues(SimpleStatement statement, StatementWriter out) {
            out.resetRemainingBoundValues();
            if (statement.valuesCount() > 0) {
                out.appendBoundValuesStart();
                if (statement.usesNamedValues()) {
                    boolean first = true;
                    for (String valueName : statement.getValueNames()) {
                        if (first)
                            first = false;
                        else
                            out.appendListElementSeparator();
                        out.appendBoundValue(valueName, statement.getObject(valueName), null);
                        if (out.maxAppendedBoundValuesExceeded())
                            break;
                    }
                } else {
                    for (int i = 0; i < statement.valuesCount(); i++) {
                        if (i > 0)
                            out.appendListElementSeparator();
                        out.appendBoundValue(i, statement.getObject(i), null);
                        if (out.maxAppendedBoundValuesExceeded())
                            break;
                    }
                }
                out.appendBoundValuesEnd();
            }
        }

    }

    public static class BuiltStatementPrinter extends AbstractRegularStatementPrinter<BuiltStatement> {

        public BuiltStatementPrinter() {
            super(BuiltStatement.class);
        }

        @Override
        protected void printHeader(BuiltStatement statement, StatementWriter out) {
            super.printHeader(statement, out);
            out.appendSummaryStart();
            out.appendBoundValuesCount(statement.valuesCount(out.getCodecRegistry()));
            out.appendSummaryEnd();
        }

        @Override
        protected void printBoundValues(BuiltStatement statement, StatementWriter out) {
            out.resetRemainingBoundValues();
            if (statement.valuesCount(out.getCodecRegistry()) > 0) {
                out.appendBoundValuesStart();
                // BuiltStatement does not use named values
                for (int i = 0; i < statement.valuesCount(out.getCodecRegistry()); i++) {
                    if (i > 0)
                        out.appendListElementSeparator();
                    out.appendBoundValue(i, statement.getObject(i), null);
                    if (out.maxAppendedBoundValuesExceeded())
                        break;
                }
                out.appendBoundValuesEnd();
            }
        }

    }

    public static class BoundStatementPrinter extends StatementPrinterBase<BoundStatement> {

        public BoundStatementPrinter() {
            super(BoundStatement.class);
        }

        @Override
        protected void printHeader(BoundStatement statement, StatementWriter out) {
            super.printHeader(statement, out);
            out.appendSummaryStart();
            ColumnDefinitions metadata = statement.preparedStatement().getVariables();
            out.appendBoundValuesCount(metadata.size());
            out.appendSummaryEnd();
        }

        @Override
        protected void printQueryString(BoundStatement statement, StatementWriter out) {
            out.resetRemainingQueryStringChars();
            out.appendQueryStringStart();
                out.appendQueryStringFragment(statement.preparedStatement().getQueryString());
            out.appendQueryStringEnd();
        }

        @Override
        protected void printBoundValues(BoundStatement statement, StatementWriter out) {
            out.resetRemainingBoundValues();
            if (statement.preparedStatement().getVariables().size() > 0) {
                out.appendBoundValuesStart();
                ColumnDefinitions metadata = statement.preparedStatement().getVariables();
                if (metadata.size() > 0) {
                    for (int i = 0; i < metadata.size(); i++) {
                        if (i > 0)
                            out.appendListElementSeparator();
                        if (statement.isSet(i))
                            out.appendBoundValue(metadata.getName(i), statement.wrapper.values[i], metadata.getType(i));
                        else
                            out.appendUnsetBoundValue(metadata.getName(i));
                        if (out.maxAppendedBoundValuesExceeded())
                            break;
                    }
                }
                out.appendBoundValuesEnd();
            }
        }

    }

    public static class BatchStatementPrinter implements StatementPrinter<BatchStatement> {

        @Override
        public Class<BatchStatement> getSupportedStatementClass() {
            return BatchStatement.class;
        }

        @Override
        public void print(BatchStatement statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            out.appendClassNameAndHashCode(statement);
            out.appendSummaryStart();
            out.appendBatchType(getBatchType(statement));
            out.appendListElementSeparator();
            out.appendInnerStatementsCount(statement.size());
            out.appendListElementSeparator();
            int totalBoundValuesCount = 0;
            for (List<ByteBuffer> values : getValues(statement, out)) {
                totalBoundValuesCount += values.size();
            }
            out.appendBoundValuesCount(totalBoundValuesCount);
            out.appendSummaryEnd();
            if (verbosity.ordinal() >= StatementFormatVerbosity.NORMAL.ordinal()) {
                out.getBuffer().append(' ');
                int i = 1;
                for (Statement stmt : statement.getStatements()) {
                    if (i > 1)
                        out.appendListElementSeparator();
                    if (i > out.getLimits().maxInnerStatements) {
                        out.appendFurtherInnerStatementsOmitted();
                        break;
                    }
                    out.getBuffer().append(i++);
                    out.appendNameValueSeparator();
                    StatementPrinter<? super Statement> printer = out.getPrinterRegistry().findPrinter(stmt);
                    printer.print(stmt, out, verbosity);
                }
            }
        }

        protected BatchStatement.Type getBatchType(BatchStatement statement) {
            return statement.batchType;
        }

        protected List<List<ByteBuffer>> getValues(BatchStatement statement, StatementWriter out) {
            return statement.getIdAndValues(out.getProtocolVersion(), out.getCodecRegistry()).values;
        }

    }

    public static class StatementWrapperPrinter implements StatementPrinter<StatementWrapper> {

        @Override
        public Class<StatementWrapper> getSupportedStatementClass() {
            return StatementWrapper.class;
        }

        @Override
        public void print(StatementWrapper statement, StatementWriter out, StatementFormatVerbosity verbosity) {
            Statement wrappedStatement = statement.getWrappedStatement();
            StatementPrinter<? super Statement> printer = out.getPrinterRegistry().findPrinter(wrappedStatement);
            printer.print(wrappedStatement, out, verbosity);
        }

    }

    public static class SchemaStatementPrinter extends AbstractRegularStatementPrinter<SchemaStatement> {

        public SchemaStatementPrinter() {
            super(SchemaStatement.class);
        }

    }

    public static class RegularStatementPrinter extends AbstractRegularStatementPrinter<RegularStatement> {

        public RegularStatementPrinter() {
            super(RegularStatement.class);
        }

    }

    public static class DefaultStatementPrinter extends StatementPrinterBase<Statement> {

        public DefaultStatementPrinter() {
            super(Statement.class);
        }

    }

    /**
     * Helper class to build {@link StatementFormatter} instances with a fluent API.
     */
    public static class Builder {

        private final List<StatementPrinter> printers = new ArrayList<StatementPrinter>();
        private int maxQueryStringLength = DEFAULT_MAX_QUERY_STRING_LENGTH;
        private int maxBoundValueLength = DEFAULT_MAX_BOUND_VALUE_LENGTH;
        private int maxBoundValues = DEFAULT_MAX_BOUND_VALUES;
        private int maxInnerStatements = DEFAULT_MAX_INNER_STATEMENTS;
        private String summaryStart = DEFAULT_SUMMARY_START;
        private String summaryEnd = DEFAULT_SUMMARY_END;
        private String queryStringStart = DEFAULT_QUERY_STRING_START;
        private String queryStringEnd = DEFAULT_QUERY_STRING_END;
        private String boundValuesCount = DEFAULT_BOUND_VALUES_COUNT;
        private String statementsCount = DEFAULT_INNER_STATEMENTS_COUNT;
        private String boundValuesStart = DEFAULT_BOUND_VALUES_START;
        private String boundValuesEnd = DEFAULT_BOUND_VALUES_END;
        private String truncatedOutput = DEFAULT_TRUNCATED_OUTPUT_MESSAGE;
        private String furtherBoundValuesOmitted = DEFAULT_FURTHER_BOUND_VALUES_OMITTED;
        private String furtherInnerStatementsOmitted = DEFAULT_FURTHER_INNER_STATEMENTS_OMITTED;
        private String nullBoundValueValue = DEFAULT_NULL_BOUND_VALUE;
        private String unsetBoundValue = DEFAULT_UNSET_BOUND_VALUE;
        private String listElementSeparator = DEFAULT_LIST_ELEMENT_SEPARATOR;
        private String nameValueSeparator = DEFAULT_NAME_VALUE_SEPARATOR;

        private Builder() {
        }

        // limits

        public Builder withMaxQueryStringLength(int maxQueryStringLength) {
            if (maxQueryStringLength <= 0 && maxQueryStringLength != UNLIMITED)
                throw new IllegalArgumentException(String.format("Invalid maxQueryStringLength, should be > 0, got %s", maxQueryStringLength));
            this.maxQueryStringLength = maxQueryStringLength;
            return this;
        }

        public Builder withMaxBoundValueLength(int maxBoundValueLength) {
            if (maxBoundValueLength <= 0)
                throw new IllegalArgumentException(String.format("Invalid maxBoundValueLength, should be > 0, got %s", maxBoundValueLength));
            this.maxBoundValueLength = maxBoundValueLength;
            return this;
        }

        public Builder withMaxBoundValues(int maxBoundValues) {
            if (maxBoundValues <= 0)
                throw new IllegalArgumentException(String.format("Invalid maxBoundValues, should be > 0, got %s", maxBoundValues));
            this.maxBoundValues = maxBoundValues;
            return this;
        }

        public Builder withMaxInnerStatements(int maxInnerStatements) {
            if (maxInnerStatements <= 0)
                throw new IllegalArgumentException(String.format("Invalid maxInnerStatements, should be > 0, got %s", maxInnerStatements));
            this.maxInnerStatements = maxInnerStatements;
            return this;
        }

        public Builder withUnlimitedQueryStringLength() {
            this.maxQueryStringLength = UNLIMITED;
            return this;
        }

        public Builder withUnlimitedBoundValueLength() {
            this.maxBoundValueLength = UNLIMITED;
            return this;
        }

        public Builder withUnlimitedBoundValues() {
            this.maxBoundValues = UNLIMITED;
            return this;
        }

        public Builder withUnlimitedInnerStatements() {
            this.maxInnerStatements = UNLIMITED;
            return this;
        }

        // symbols

        public Builder withSummaryStart(String summaryStart) {
            this.summaryStart = summaryStart;
            return this;
        }

        public Builder withSummaryEnd(String summaryEnd) {
            this.summaryEnd = summaryEnd;
            return this;
        }

        public Builder withQueryStringStart(String queryStringStart) {
            this.queryStringStart = queryStringStart;
            return this;
        }

        public Builder withQueryStringEnd(String queryStringEnd) {
            this.queryStringEnd = queryStringEnd;
            return this;
        }

        public Builder withBoundValuesStart(String boundValuesStart) {
            this.boundValuesStart = boundValuesStart;
            return this;
        }

        public Builder withBoundValuesEnd(String boundValuesEnd) {
            this.boundValuesEnd = boundValuesEnd;
            return this;
        }

        public Builder withValuesCount(String valuesCount) {
            this.boundValuesCount = valuesCount;
            return this;
        }

        public Builder withInnerStatementsCount(String statementsCount) {
            this.statementsCount = statementsCount;
            return this;
        }

        public Builder withTruncatedOutput(String truncatedOutput) {
            this.truncatedOutput = truncatedOutput;
            return this;
        }

        public Builder withFurtherBoundValuesOmitted(String furtherBoundValuesOmitted) {
            this.furtherBoundValuesOmitted = furtherBoundValuesOmitted;
            return this;
        }

        public Builder withFurtherInnerStatementsOmitted(String furtherInnerStatementsOmitted) {
            this.furtherInnerStatementsOmitted = furtherInnerStatementsOmitted;
            return this;
        }

        public Builder withNullBoundValue(String nullBoundValueValue) {
            this.nullBoundValueValue = nullBoundValueValue;
            return this;
        }

        public Builder withUnsetBoundValue(String unsetBoundValue) {
            this.unsetBoundValue = unsetBoundValue;
            return this;
        }

        public Builder withListElementSeparator(String listElementSeparator) {
            this.listElementSeparator = listElementSeparator;
            return this;
        }

        public Builder withNameValueSeparator(String nameValueSeparator) {
            this.nameValueSeparator = nameValueSeparator;
            return this;
        }

        public Builder addStatementPrinter(StatementPrinter<?> printer) {
            printers.add(printer);
            return this;
        }

        public Builder addStatementPrinters(StatementPrinter<?>... printers) {
            this.printers.addAll(Arrays.asList(printers));
            return this;
        }

        /**
         * Build the {@link StatementFormatter} instance.
         *
         * @return the {@link StatementFormatter} instance.
         * @throws IllegalArgumentException if the builder is unable to build a valid instance due to incorrect settings.
         */
        public StatementFormatter build() {
            StatementPrinterRegistry registry = new StatementPrinterRegistry();
            registerDefaultPrinters(registry);
            for (StatementPrinter printer : printers) {
                registry.register(printer);
            }
            StatementWriterLimits limits = new StatementWriterLimits(
                    maxQueryStringLength, maxBoundValueLength, maxBoundValues, maxInnerStatements);
            StatementWriterSymbols symbols = new StatementWriterSymbols(
                    summaryStart, summaryEnd, boundValuesCount, statementsCount,
                    queryStringStart, queryStringEnd,
                    boundValuesStart, boundValuesEnd,
                    truncatedOutput, furtherBoundValuesOmitted, furtherInnerStatementsOmitted,
                    nullBoundValueValue, unsetBoundValue,
                    listElementSeparator, nameValueSeparator);
            return new StatementFormatter(registry, limits, symbols);
        }

        private void registerDefaultPrinters(StatementPrinterRegistry registry) {
            registry.register(new DefaultStatementPrinter());
            registry.register(new SimpleStatementPrinter());
            registry.register(new RegularStatementPrinter());
            registry.register(new BuiltStatementPrinter());
            registry.register(new SchemaStatementPrinter());
            registry.register(new BoundStatementPrinter());
            registry.register(new BatchStatementPrinter());
            registry.register(new StatementWrapperPrinter());
        }

    }

    private final StatementPrinterRegistry registry;
    private final StatementWriterLimits limits;
    private final StatementWriterSymbols symbols;

    private StatementFormatter(StatementPrinterRegistry registry, StatementWriterLimits limits, StatementWriterSymbols symbols) {
        this.registry = registry;
        this.limits = limits;
        this.symbols = symbols;
    }

    /**
     * Formats the given {@link Statement statement}.
     *
     * @param statement       The statement to format.
     * @param verbosity       The verbosity to use.
     * @param protocolVersion The protocol version in use.
     * @param codecRegistry   The codec registry in use.
     * @return The statement as a formatted string.
     * @throws StatementFormatException if the formatting failed.
     */
    public <S extends Statement> String format(
            S statement, StatementFormatVerbosity verbosity,
            ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        try {
            StatementPrinter<? super S> printer = registry.findPrinter(statement);
            assert printer != null : "Could not find printer for statement class " + statement.getClass();
            StatementWriter out = new StatementWriter(registry, limits, symbols, protocolVersion, codecRegistry);
            printer.print(statement, out, verbosity);
            return out.toString().trim();
        } catch (RuntimeException e) {
            throw new StatementFormatException(statement, verbosity, protocolVersion, codecRegistry, e);
        }
    }

}
