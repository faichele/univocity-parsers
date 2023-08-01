/*******************************************************************************
 * Copyright 2014 Univocity Software Pty Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.univocity.parsers.csv;

import com.univocity.parsers.common.*;
import com.univocity.parsers.common.input.EOFException;
import com.univocity.parsers.common.input.*;

// import javax.swing.text.html.parser.Parser;
import java.io.*;
import java.util.*;

import static com.univocity.parsers.csv.UnescapedQuoteHandling.*;

/**
 * A very fast CSV parser implementation.
 *
 * @author Univocity Software Pty Ltd - <a href="mailto:parsers@univocity.com">parsers@univocity.com</a>
 * @see CsvFormat
 * @see CsvParserSettings
 * @see CsvWriter
 * @see AbstractParser
 */
public final class CsvParser extends AbstractParser<CsvParserSettings> {

	private boolean parseUnescapedQuotes;
	private boolean parseUnescapedQuotesUntilDelimiter;
	private boolean backToDelimiter;
	private final boolean doNotEscapeUnquotedValues;
	private final boolean keepEscape;
	private final boolean keepQuotes;

	private boolean unescaped;
	private char prev;
	private char delimiter;
	private char[] multiDelimiter;
	private char quote;
	private char quoteEscape;
	private char escapeEscape;
	private char newLine;
	private final DefaultCharAppender whitespaceAppender;
	private final boolean normalizeLineEndingsInQuotes;
	private UnescapedQuoteHandling quoteHandling;
	private final String nullValue;
	private final int maxColumnLength;
	private final String emptyValue;
	private final boolean trimQuotedLeading;
	private final boolean trimQuotedTrailing;
	private char[] delimiters;
	private int match = 0;
	private int formatDetectorRowSampleCount;

	private Map<Long, Map<Long, String>> csvPotentialRecordLines;
	private Map<Long, Map<Long, String>> csvParsedRecordLines;

	/**
	 * The CsvParser supports all settings provided by {@link CsvParserSettings}, and requires this configuration to be properly initialized.
	 *
	 * @param settings the parser configuration
	 */
	public CsvParser(CsvParserSettings settings) {
		super(settings);
		parseUnescapedQuotes = settings.isParseUnescapedQuotes();
		parseUnescapedQuotesUntilDelimiter = settings.isParseUnescapedQuotesUntilDelimiter();
		doNotEscapeUnquotedValues = !settings.isEscapeUnquotedValues();
		keepEscape = settings.isKeepEscapeSequences();
		keepQuotes = settings.getKeepQuotes();
		normalizeLineEndingsInQuotes = settings.isNormalizeLineEndingsWithinQuotes();
		nullValue = settings.getNullValue();
		emptyValue = settings.getEmptyValue();
		maxColumnLength = settings.getMaxCharsPerColumn();
		trimQuotedTrailing = settings.getIgnoreTrailingWhitespacesInQuotes();
		trimQuotedLeading = settings.getIgnoreLeadingWhitespacesInQuotes();
		formatDetectorRowSampleCount = settings.getFormatDetectorRowSampleCount();
		updateFormat(settings.getFormat());

		whitespaceAppender = new ExpandingCharAppender(10, "", whitespaceRangeStart);

		csvPotentialRecordLines = new TreeMap<Long, Map<Long, String>>();
		csvParsedRecordLines = new TreeMap<Long, Map<Long, String>>();

		this.quoteHandling = settings.getUnescapedQuoteHandling();
		if (quoteHandling == null) {
			if (parseUnescapedQuotes) {
				if (parseUnescapedQuotesUntilDelimiter) {
					quoteHandling = STOP_AT_DELIMITER;
				} else {
					quoteHandling = STOP_AT_CLOSING_QUOTE;
				}
			} else {
				quoteHandling = RAISE_ERROR;
			}
		} else {
			backToDelimiter = quoteHandling == BACK_TO_DELIMITER;
			parseUnescapedQuotesUntilDelimiter = quoteHandling == STOP_AT_DELIMITER || quoteHandling == SKIP_VALUE || backToDelimiter;
			parseUnescapedQuotes = quoteHandling != RAISE_ERROR;
		}
	}


	@Override
	protected final ParseRecordResult parseRecord() {
		System.out.println("input.lineCount() = " + input.lineCount());
		boolean readingFirstLine = (input.lineCount() == 0);

		String currentLine = "";
		if (readingFirstLine) {
			String firstLine = input.skipString(input.getChar(), input.getLineSeparator()[0]);
			if (firstLine.length() > 0) {
				firstLine = firstLine.substring(0, firstLine.length() - 1);
			}
			System.out.println("Read first line: " + firstLine);
			currentLine = firstLine;
		} else {
			currentLine = input.previousLine();
		}

		long parserLineCount = input.lineCount();
		System.out.println("CsvParser parseRecord() Line " + parserLineCount + ": " + currentLine);
		if (multiDelimiter == null) {
			int delimiterIndex = currentLine.indexOf(delimiter);
			if (delimiterIndex == -1) {
				/*if (readingFirstLine)
					return ParseRecordResult.CONTINUE_PARSING;
				else
					return ParseRecordResult.RECORD_PARSED;*/
				String skippedText = input.skipString(input.getChar(), input.getLineSeparator()[0]);
				System.out.println("Skipped non-record line: " + skippedText);
				return ParseRecordResult.CONTINUE_PARSING;
			} else {
				long delimiterCount = currentLine.chars().filter(ch -> ch == delimiter).count();
				if (!csvPotentialRecordLines.containsKey(delimiterCount)) {
					System.out.println("Adding new delimiter count entry (potential records): " + delimiterCount);
					csvPotentialRecordLines.put(delimiterCount, new TreeMap<Long, String>());
				}

				csvPotentialRecordLines.get(delimiterCount).put(parserLineCount, currentLine);
			}

			ParseRecordResult singleRecordResult = parseSingleDelimiterRecord();
			return singleRecordResult;
		} else {
			parseMultiDelimiterRecord();
		}

		return ParseRecordResult.CONTINUE_PARSING;
	}

	private final ParseRecordResult parseSingleDelimiterRecord() {
		long valuesParsed = 0;
		if (ch <= ' ' && ignoreLeadingWhitespace && whitespaceRangeStart < ch) {
			ch = input.skipWhitespace(ch, delimiter, quote);
			currentLine.add(ch);
		}

		while (ch != newLine) {
			currentLine.add(ch);

			if (ch <= ' ' && ignoreLeadingWhitespace && whitespaceRangeStart < ch) {
				ch = input.skipWhitespace(ch, delimiter, quote);
				currentLine.add(ch);
			}

			if (ch == delimiter || ch == newLine) {
				output.emptyParsed();

				if (ch == newLine) {
					String previousLine = input.previousLine();
					System.out.println("Line parsed in CsvParser (134): " + currentLine.toString() +
							" -- prev. line from input: " + previousLine);
					currentLine.clear();
				}
			} else {
				unescaped = false;
				prev = '\0';
				if (ch == quote) {
					input.enableNormalizeLineEndings(normalizeLineEndingsInQuotes);
					int len = output.appender.length();
					if (len == 0) {
						String value = input.getQuotedString(quote, quoteEscape, escapeEscape, maxColumnLength, delimiter, newLine, keepQuotes, keepEscape, trimQuotedLeading, trimQuotedTrailing);
						if (value != null) {
							output.valueParsed(value == "" ? emptyValue : value);
							System.out.println("CSV parser -- value parsed (line 196): \"" + value + "\"");
							valuesParsed++;

							input.enableNormalizeLineEndings(true);

							for (int l = 0; l < value.length(); l++) {
								currentLine.add(value.charAt(l));
							}

							try {
								ch = input.nextChar();
								currentLine.add(ch);

								if (ch == delimiter) {
									try {
										ch = input.nextChar();
										currentLine.add(ch);

										if (ch == newLine) {
											output.emptyParsed();
											String previousLine = input.previousLine();
											System.out.println("Line parsed in CsvParser (167): " + currentLine.toString() +
													" -- prev. line from input: " + previousLine);

											currentLine.clear();
										}
									} catch (EOFException e) {
										output.emptyParsed();

										String previousLine = input.previousLine();
										System.out.println("Line parsed in CsvParser (173): " + currentLine.toString() +
												" -- prev. line from input: " + previousLine);
										currentLine.clear();

										return ParseRecordResult.STOP_PARSING;
									}
								}
							} catch (EOFException e) {
								return ParseRecordResult.STOP_PARSING;
							}
							System.out.println("CSVParser BailOut line 188");
							continue;
						}
					} else if (len == -1 && input.skipQuotedString(quote, quoteEscape, delimiter, newLine)) {
						output.valueParsed();
						System.out.println("CSV parser -- quoted value parsed (line 242)");
						valuesParsed++;

						try {
							ch = input.nextChar();
							currentLine.add(ch);

							if (ch == delimiter) {
								try {
									ch = input.nextChar();
									currentLine.add(ch);

									if (ch == newLine) {
										output.emptyParsed();

										String previousLine = input.previousLine();
										System.out.println("Line parsed in CsvParser (205): " + currentLine.toString() +
												" -- prev. line from input: " + previousLine);
										currentLine.clear();
									}
								} catch (EOFException e) {
									output.emptyParsed();

									String previousLine = input.previousLine();
									System.out.println("Line parsed in CsvParser (213): " + currentLine.toString() +
											" -- prev. line from input: " + previousLine);
									currentLine.clear();

									return ParseRecordResult.STOP_PARSING;
								}
							}
						} catch (EOFException e) {
							String previousLine = input.previousLine();
							System.out.println("Line parsed in CsvParser (222): " + currentLine.toString() +
									" -- prev. line from input: " + previousLine);
							currentLine.clear();

							return ParseRecordResult.STOP_PARSING;
						}
						System.out.println("CSVParser BailOut line 228");
						continue;
					}
					output.trim = trimQuotedTrailing;
					parseQuotedValue();
					input.enableNormalizeLineEndings(true);
					if (!(unescaped && quoteHandling == BACK_TO_DELIMITER && output.appender.length() == 0)) {
						output.valueParsed();
						System.out.println("CSV parser -- quoted value parsed (line 288)");

						valuesParsed++;
					}
				} else if (doNotEscapeUnquotedValues) {
					String value = null;
					int len = output.appender.length();
					if (len == 0) {
						value = input.getString(ch, delimiter, ignoreTrailingWhitespace, nullValue, maxColumnLength);
					}
					if (value != null) {
						output.valueParsed(value);
						System.out.println("CSV parser -- value parsed (line 300): " + value);
						valuesParsed++;

						for (int l = 0; l < value.length(); l++) {
							currentLine.add(value.charAt(l));
						}

						ch = input.getChar();
						currentLine.add(ch);
					} else {
						if (len != -1) {
							output.trim = ignoreTrailingWhitespace;
							int oldPos = output.appender.length() + output.appender.whitespaceCount();
							ch = output.appender.appendUntil(ch, input, delimiter, newLine);
							int newPos = output.appender.length() + output.appender.whitespaceCount();
							for (int k = 0; k < newPos - oldPos; k++) {
								currentLine.add(output.appender.charAt(oldPos + k));
							}
							currentLine.add(ch);
						} else {
							int oldPos = output.appender.length() + output.appender.whitespaceCount();
							ch = output.appender.appendUntil(ch, input, delimiter, newLine);
							int newPos = output.appender.length() + output.appender.whitespaceCount();
							for (int k = 0; k < newPos - oldPos; k++) {
								currentLine.add(output.appender.charAt(oldPos + k));
							}
							currentLine.add(ch);
						}
						output.valueParsed();
						System.out.println("CSV parser -- quoted value parsed (line 329)");
						valuesParsed++;
					}
				} else {
					output.trim = ignoreTrailingWhitespace;
					parseValueProcessingEscape();
					output.valueParsed();
					System.out.println("CSV parser -- quoted value parsed (line 336)");
					valuesParsed++;
				}
			}
			if (ch != newLine) {
				ch = input.nextChar();
				currentLine.add(ch);

				if (ch == newLine) {
					String previousLine = input.previousLine();
					System.out.println("Line parsed in CsvParser (284): " + currentLine.toString() +
							" -- prev. line from input: " + previousLine);
					currentLine.clear();

					output.emptyParsed();
				}
			}
		}

		System.out.println("Line " + input.currentLine() + ": Values parsed from CSV record = " + valuesParsed);
		if (!csvParsedRecordLines.containsKey(valuesParsed)) {
			System.out.println("Adding new delimiter count entry (parsed records): " + valuesParsed);
			csvParsedRecordLines.put(valuesParsed, new TreeMap<Long, String>());
		}

		csvParsedRecordLines.get(valuesParsed).put(input.lineCount(), input.previousLine());

		return ParseRecordResult.RECORD_PARSED;
	}

	private void skipValue() {
		output.appender.reset();
		output.appender = NoopCharAppender.getInstance();
		if (multiDelimiter == null) {
			// ch = NoopCharAppender.getInstance().appendUntil(ch, input, delimiter, newLine);

			int oldPos = NoopCharAppender.getInstance().length() + NoopCharAppender.getInstance().whitespaceCount();
			ch = NoopCharAppender.getInstance().appendUntil(ch, input, delimiter, newLine);
			int newPos = NoopCharAppender.getInstance().length() + NoopCharAppender.getInstance().whitespaceCount();
			for (int k = 0; k < newPos - oldPos; k++) {
				currentLine.add(NoopCharAppender.getInstance().charAt(oldPos + k));
			}
		} else {
			for (; match < multiDelimiter.length && ch != newLine; ch = input.nextChar()) {
				currentLine.add(ch);

				if (multiDelimiter[match] == ch) {
					match++;
				} else {
					match = 0;
				}
			}
		}
	}

	private void handleValueSkipping(boolean quoted) {
		switch (quoteHandling) {
			case SKIP_VALUE:
				skipValue();
				break;
			case RAISE_ERROR:
				throw new TextParsingException(context, "Unescaped quote character '" + quote
						+ "' inside " + (quoted ? "quoted" : "") + " value of CSV field. To allow unescaped quotes, set 'parseUnescapedQuotes' to 'true' in the CSV parser settings. Cannot parse CSV input.");
		}
	}

	private void handleUnescapedQuoteInValue() {
		switch (quoteHandling) {
			case BACK_TO_DELIMITER:
			case STOP_AT_CLOSING_QUOTE:
			case STOP_AT_DELIMITER:
				output.appender.append(quote);
				currentLine.add(quote);

				prev = ch;
				parseValueProcessingEscape();
				break;
			default:
				handleValueSkipping(false);
				break;
		}
	}

	private int nextDelimiter() {
		if (multiDelimiter == null) {
			return output.appender.indexOfAny(delimiters, 0);
		} else {
			int lineEnd = output.appender.indexOf(newLine, 0);
			int delimiter = output.appender.indexOf(multiDelimiter, 0);

			return lineEnd != -1 && lineEnd < delimiter ? lineEnd : delimiter;
		}
	}

	private boolean handleUnescapedQuote() {
		unescaped = true;
		switch (quoteHandling) {
			case BACK_TO_DELIMITER:
				int pos;
				int lastPos = 0;
				while ((pos = nextDelimiter()) != -1) {
					lastPos = pos;
					String value = output.appender.substring(0, pos);
					if (keepQuotes && output.appender.charAt(pos - 1) == quote) {
						value += quote;
					}
					output.valueParsed(value);

					for (int k = 0; k < value.length(); k++) {
						currentLine.add(value.charAt(k));
					}

					if (output.appender.charAt(pos) == newLine) {
						output.pendingRecords.add(output.rowParsed());
						output.appender.remove(0, pos + 1);

						String previousLine = input.previousLine();
						System.out.println("Line parsed in CsvParser (381): " + currentLine.toString() +
								" -- prev. line from input: " + previousLine);
						currentLine.clear();

						continue;
					}
					if (multiDelimiter == null) {
						output.appender.remove(0, pos + 1);
					} else {
						output.appender.remove(0, pos + multiDelimiter.length);
					}
				}
				if (keepQuotes && input.lastIndexOf(quote) > lastPos) {
					output.appender.append(quote);
					currentLine.add(quote);
				}
				output.appender.append(ch);
				currentLine.add(ch);

				if (ch == newLine) {
					String previousLine = input.previousLine();
					System.out.println("Line parsed in CsvParser (402): " + currentLine.toString() +
							" -- prev. line from input: " + previousLine);
					currentLine.clear();
				}

				prev = '\0';
				if (multiDelimiter == null) {
					parseQuotedValue();
				} else {
					parseQuotedValueMultiDelimiter();
				}
				return true;
			case STOP_AT_CLOSING_QUOTE:
			case STOP_AT_DELIMITER:
				output.appender.append(quote);
				currentLine.add(quote);
				output.appender.append(ch);
				currentLine.add(ch);

				if (ch == newLine) {
					String previousLine = input.previousLine();
					System.out.println("Line parsed in CsvParser (423): " + currentLine.toString() +
							" -- prev. line from input: " + previousLine);
				}

				prev = ch;
				if (multiDelimiter == null) {
					parseQuotedValue();
				} else {
					parseQuotedValueMultiDelimiter();
				}
				return true; //continue;
			default:
				handleValueSkipping(true);
				return false;
		}
	}

	private void processQuoteEscape() {
		if (ch == quoteEscape && prev == escapeEscape && escapeEscape != '\0') {
			if (keepEscape) {
				output.appender.append(escapeEscape);
				currentLine.add(escapeEscape);
			}
			output.appender.append(quoteEscape);
			currentLine.add(quoteEscape);

			ch = '\0';
		} else if (prev == quoteEscape) {
			if (ch == quote) {
				if (keepEscape) {
					output.appender.append(quoteEscape);
					currentLine.add(quoteEscape);
				}
				output.appender.append(quote);
				currentLine.add(quote);

				ch = '\0';
			} else {
				output.appender.append(prev);
				currentLine.add(prev);
			}
		} else if (ch == quote && prev == quote) {
			output.appender.append(quote);
			currentLine.add(quote);
		} else if (prev == quote) { //unescaped quote detected
			handleUnescapedQuoteInValue();
		}
	}

	private void parseValueProcessingEscape() {
		while (ch != delimiter && ch != newLine) {
			if (ch != quote && ch != quoteEscape) {
				if (prev == quote) { //unescaped quote detected
					handleUnescapedQuoteInValue();
					return;
				}
				output.appender.append(ch);
			} else {
				processQuoteEscape();
			}
			prev = ch;
			ch = input.nextChar();
			currentLine.add(ch);

			if (ch == newLine) {
				String previousLine = input.previousLine();
				System.out.println("Line parsed in CsvParser (489): " + currentLine.toString() +
						" -- prev. line from input: " + previousLine);
			}
		}
	}

	private ParseRecordResult parseQuotedValue() {
		if (prev != '\0' && parseUnescapedQuotesUntilDelimiter) {
			if (quoteHandling == SKIP_VALUE) {
				skipValue();
				return ParseRecordResult.CONTINUE_PARSING;
			}
			if (!keepQuotes) {
				output.appender.prepend(quote); // TODO: prepend?
			}
			ch = input.nextChar();
			currentLine.add(ch);

			output.trim = ignoreTrailingWhitespace;

			int oldPos = output.appender.length() + output.appender.whitespaceCount();
			ch = output.appender.appendUntil(ch, input, delimiter, newLine);
			int newPos = output.appender.length() + output.appender.whitespaceCount();
			for (int k = 0; k < newPos - oldPos; k++) {
				currentLine.add(NoopCharAppender.getInstance().charAt(oldPos + k));
			}
		} else {
			if (keepQuotes && prev == '\0') {
				output.appender.append(quote);
				currentLine.add(quote);
			}
			ch = input.nextChar();
			currentLine.add(ch);

			if (trimQuotedLeading && ch <= ' ' && output.appender.length() == 0) {
				while ((ch = input.nextChar()) <= ' ') {
					currentLine.add(ch);
				} //;
			}

			while (true) {
				if (prev == quote && (ch <= ' ' && whitespaceRangeStart < ch || ch == delimiter || ch == newLine)) {
					if (ch == newLine) {
						String previousLine = input.previousLine();
						System.out.println("Line parsed in CsvParser (533): " + currentLine.toString() +
								" -- prev. line from input: " + previousLine);
						currentLine.clear();
					}

					break;
				}

				if (ch != quote && ch != quoteEscape) {
					if (prev == quote) { //unescaped quote detected
						if (handleUnescapedQuote()) {
							if (quoteHandling == SKIP_VALUE) {
								break;
							} else {
								return ParseRecordResult.CONTINUE_PARSING;
							}
						} else {
							return ParseRecordResult.STOP_PARSING;
						}
					}
					if (prev == quoteEscape && quoteEscape != '\0') {
						output.appender.append(quoteEscape);
						currentLine.add(quoteEscape);
					}
					int oldPos = output.appender.length() + output.appender.whitespaceCount();
					ch = output.appender.appendUntil(ch, input, quote, quoteEscape, escapeEscape);
					int newPos = output.appender.length() + output.appender.whitespaceCount();
					for (int k = 0; k < newPos - oldPos; k++) {
						currentLine.add(output.appender.charAt(oldPos + k));
					}

					prev = ch;
					ch = input.nextChar();
					currentLine.add(ch);
				} else {
					processQuoteEscape();
					prev = ch;
					ch = input.nextChar();
					currentLine.add(ch);
					if (unescaped && (ch == delimiter || ch == newLine)) {
						if (ch == newLine) {
							String previousLine = input.previousLine();
							System.out.println("Line parsed in CsvParser (574): " + currentLine.toString() +
									" -- prev. line from input: " + previousLine);
							currentLine.clear();
						}

						return ParseRecordResult.RECORD_PARSED;
					}
				}
			}

			// handles whitespaces after quoted value: whitespaces are ignored. Content after whitespaces may be parsed if 'parseUnescapedQuotes' is enabled.
			if (ch != delimiter && ch != newLine && ch <= ' ' && whitespaceRangeStart < ch) {
				whitespaceAppender.reset();
				do {
					//saves whitespaces after value
					whitespaceAppender.append(ch);
					ch = input.nextChar();
					currentLine.add(ch);

					//found a new line, go to next record.
					if (ch == newLine) {
						String previousLine = input.previousLine();
						System.out.println("Line parsed in CsvParser (597): " + currentLine.toString() +
								" -- prev. line from input: " + previousLine);
						currentLine.clear();

						if (keepQuotes) {
							output.appender.append(quote);
							currentLine.add(quote);
						}
						return ParseRecordResult.CONTINUE_PARSING;
					}
				} while (ch <= ' ' && whitespaceRangeStart < ch && ch != delimiter);

				//there's more stuff after the quoted value, not only empty spaces.
				if (ch != delimiter && parseUnescapedQuotes) {
					if (output.appender instanceof DefaultCharAppender) {
						//puts the quote before whitespaces back, then restores the whitespaces
						output.appender.append(quote);
						currentLine.add(quote);

						((DefaultCharAppender) output.appender).append(whitespaceAppender);
					}
					//the next character is not the escape character, put it there
					if (parseUnescapedQuotesUntilDelimiter || ch != quote && ch != quoteEscape) {
						output.appender.append(ch);
						currentLine.add(ch);
					}

					//sets this character as the previous character (may be escaping)
					//calls recursively to keep parsing potentially quoted content
					prev = ch;
					return parseQuotedValue();
				} else if (keepQuotes) {
					output.appender.append(quote);
					currentLine.add(quote);
				}
			} else if (keepQuotes) {
				output.appender.append(quote);
				currentLine.add(quote);
			}

			if (ch != delimiter && ch != newLine) {
				throw new TextParsingException(context, "Unexpected character '" + ch + "' following quoted value of CSV field. Expecting '" + delimiter + "'. Cannot parse CSV input.");
			}
		}

		return ParseRecordResult.CONTINUE_PARSING;
	}

	@Override
	protected final InputAnalysisProcess getInputAnalysisProcess() {
		if (settings.isDelimiterDetectionEnabled() || settings.isQuoteDetectionEnabled()) {
			return new CsvFormatDetector(formatDetectorRowSampleCount, settings, whitespaceRangeStart) {
				@Override
				protected void apply(char delimiter, char quote, char quoteEscape) {
					if (settings.isDelimiterDetectionEnabled()) {
						CsvParser.this.delimiter = delimiter;
						CsvParser.this.delimiters[0] = delimiter;

					}
					if (settings.isQuoteDetectionEnabled()) {
						CsvParser.this.quote = quote;
						CsvParser.this.quoteEscape = quoteEscape;
					}
				}
			};
		}
		return null;
	}

	/**
	 * Returns the CSV format detected when one of the following settings is enabled:
	 * <ul>
	 * <li>{@link CommonParserSettings#isLineSeparatorDetectionEnabled()}</li>
	 * <li>{@link CsvParserSettings#isDelimiterDetectionEnabled()}</li>
	 * <li>{@link CsvParserSettings#isQuoteDetectionEnabled()}</li>
	 * </ul>
	 *
	 * The detected format will be available once the parsing process is initialized (i.e. when {@link AbstractParser#beginParsing(Reader) runs}.
	 *
	 * @return the detected CSV format, or {@code null} if no detection has been enabled or if the parsing process has not been started yet.
	 */
	public final CsvFormat getDetectedFormat() {
		CsvFormat out = null;
		if (settings.isDelimiterDetectionEnabled()) {
			out = settings.getFormat().clone();
			out.setDelimiter(this.delimiter);
		}
		if (settings.isQuoteDetectionEnabled()) {
			out = out == null ? settings.getFormat().clone() : out;
			out.setQuote(quote);
			out.setQuoteEscape(quoteEscape);
		}
		if (settings.isLineSeparatorDetectionEnabled()) {
			out = out == null ? settings.getFormat().clone() : out;
			out.setLineSeparator(input.getLineSeparator());
		}
		return out;
	}

	@Override
	protected final boolean consumeValueOnEOF() {
		if (ch == quote) {
			if (prev == quote) {
				if (keepQuotes) {
					output.appender.append(quote);
					currentLine.add(quote);
				}
				return true;
			} else {
				if (!unescaped) {
					output.appender.append(quote);
					currentLine.add(quote);
				}
			}
		}
		boolean out = prev != '\0' && ch != delimiter && ch != newLine && ch != comment;
		ch = prev = '\0'; // TODO: newLine in consumeValueOnEOF?
		if (match > 0) {
			saveMatchingCharacters();
			return true;
		}
		return out;
	}

	/**
	 * Allows changing the format of the input on the fly.
	 *
	 * @param format the new format to use.
	 */
	public final void updateFormat(CsvFormat format) {
		newLine = format.getNormalizedNewline();
		multiDelimiter = format.getDelimiterString().toCharArray();
		if (multiDelimiter.length == 1) {
			multiDelimiter = null;
			delimiter = format.getDelimiter();
			delimiters = new char[]{delimiter, newLine};
		} else {
			delimiters = new char[]{multiDelimiter[0], newLine};
		}
		quote = format.getQuote();
		quoteEscape = format.getQuoteEscape();
		escapeEscape = format.getCharToEscapeQuoteEscaping();
	}

	private void skipWhitespace() {
		while (ch <= ' ' && match < multiDelimiter.length && ch != quote && whitespaceRangeStart < ch) {
			if (ch == newLine) {
				String previousLine = input.previousLine();
				System.out.println("Line parsed in CsvParser (743): " + currentLine.toString() +
						" -- prev. line from input: " + previousLine);
				currentLine.add(ch);
				break;
			}
			ch = input.nextChar();
			currentLine.add(ch);

			if (multiDelimiter[match] == ch) {
				if (matchDelimiter()) {
					output.emptyParsed();
					ch = input.nextChar();
					currentLine.add(ch);
				}
			}
		}

		saveMatchingCharacters();
	}

	private void saveMatchingCharacters() {
		if (match > 0) {
			if (match < multiDelimiter.length) {
				output.appender.append(multiDelimiter, 0, match);

				for (int k = 0; k < match; k++) {
					for (int l = 0; l < multiDelimiter.length; l++) {
						currentLine.add(multiDelimiter[l]);
					}
				}
			}
			match = 0;
		}
	}

	private boolean matchDelimiter() {
		while (ch == multiDelimiter[match]) {
			match++;
			if (match == multiDelimiter.length) {
				break;
			}
			ch = input.nextChar();
			currentLine.add(ch);
		}

		if (multiDelimiter.length == match) {
			match = 0;
			return true;
		}

		if (match > 0) {
			saveMatchingCharacters();
		}

		return false;
	}

	private boolean matchDelimiterAfterQuote() {
		while (ch == multiDelimiter[match]) {
			match++;
			if (match == multiDelimiter.length) {
				break;
			}
			ch = input.nextChar();
			currentLine.add(ch);
		}

		if (multiDelimiter.length == match) {
			match = 0;
			return true;
		}

		return false;
	}

	private ParseRecordResult parseMultiDelimiterRecord() {
		if (ch <= ' ' && ignoreLeadingWhitespace && whitespaceRangeStart < ch) {
			skipWhitespace();
		}

		while (ch != newLine) {
			if (ch <= ' ' && ignoreLeadingWhitespace && whitespaceRangeStart < ch) {
				skipWhitespace();
			}

			if (ch == newLine || matchDelimiter()) {
				if (ch == newLine) {
					String previousLine = input.previousLine();
					System.out.println("Line parsed in CsvParser (782): " + currentLine.toString() +
							" -- prev. line from input: " + previousLine);
					currentLine.clear();
				}

				output.emptyParsed();
			} else {
				unescaped = false;
				prev = '\0';
				if (ch == quote && output.appender.length() == 0) {
					input.enableNormalizeLineEndings(normalizeLineEndingsInQuotes);
					output.trim = trimQuotedTrailing;
					parseQuotedValueMultiDelimiter();
					input.enableNormalizeLineEndings(true);
					if (!(unescaped && quoteHandling == BACK_TO_DELIMITER && output.appender.length() == 0)) {
						output.valueParsed();
					}
				} else if (doNotEscapeUnquotedValues) {
					appendUntilMultiDelimiter();
					if (ignoreTrailingWhitespace) {
						output.appender.updateWhitespace();
					}
					output.valueParsed();
				} else {
					output.trim = ignoreTrailingWhitespace;
					parseValueProcessingEscapeMultiDelimiter();
					output.valueParsed();
				}
			}
			if (ch != newLine) {
				ch = input.nextChar();
				currentLine.add(ch);

				if (ch == newLine) {
					output.emptyParsed();

					String previousLine = input.previousLine();
					System.out.println("Line parsed in CsvParser (868): " + currentLine.toString() +
							" -- prev. line from input: " + previousLine);
					currentLine.clear();
				}
			}
		}

		return ParseRecordResult.CONTINUE_PARSING;
	}

	private void appendUntilMultiDelimiter() {
		while (match < multiDelimiter.length && ch != newLine) {
			if (multiDelimiter[match] == ch) {
				match++;
				if (match == multiDelimiter.length) {
					break;
				}
			} else {
				if (match > 0) {
					saveMatchingCharacters();
					continue;
				}
				output.appender.append(ch);
				currentLine.add(ch);
			}
			ch = input.nextChar();
			currentLine.add(ch);
		}
		saveMatchingCharacters();
	}

	private ParseRecordResult parseQuotedValueMultiDelimiter() {
		if (prev != '\0' && parseUnescapedQuotesUntilDelimiter) {
			if (quoteHandling == SKIP_VALUE) {
				skipValue();
				return ParseRecordResult.CONTINUE_PARSING;
			}
			if (!keepQuotes) {
				output.appender.prepend(quote);
			}
			ch = input.nextChar();
			currentLine.add(ch);

			output.trim = ignoreTrailingWhitespace;
			appendUntilMultiDelimiter();
		} else {
			if (keepQuotes && prev == '\0') {
				output.appender.append(quote);
			}
			ch = input.nextChar();
			currentLine.add(ch);

			if (trimQuotedLeading && ch <= ' ' && output.appender.length() == 0) {
				while ((ch = input.nextChar()) <= ' ') {
					currentLine.add(ch);
				} //;
			}

			while (true) {
				if (prev == quote && (ch <= ' ' && whitespaceRangeStart < ch || ch == newLine)) {
					if (ch == newLine) {
						String previousLine = input.previousLine();
						System.out.println("Line parsed in CsvParser (928): " + currentLine.toString() +
								" -- prev. line from input: " + previousLine);
						currentLine.clear();
					}

					break;
				}
				if (prev == quote && matchDelimiter()) {
					if (keepQuotes) {
						output.appender.append(quote);
						currentLine.add(quote);
					}
					return ParseRecordResult.CONTINUE_PARSING;
				}

				if (ch != quote && ch != quoteEscape) {
					if (prev == quote) { //unescaped quote detected
						if (handleUnescapedQuote()) {
							if (quoteHandling == SKIP_VALUE) {
								break;
							} else {
								return ParseRecordResult.STOP_PARSING;
							}
						} else {
							return ParseRecordResult.STOP_PARSING;
						}
					}
					if (prev == quoteEscape && quoteEscape != '\0') {
						output.appender.append(quoteEscape);
						currentLine.add(quoteEscape);
					}

					int oldPos = output.appender.length() + output.appender.whitespaceCount();
					ch = output.appender.appendUntil(ch, input, quote, quoteEscape, escapeEscape);
					int newPos = output.appender.length() + output.appender.whitespaceCount();
					for (int k = 0; k < newPos - oldPos; k++) {
						currentLine.add(NoopCharAppender.getInstance().charAt(oldPos + k));
					}

					prev = ch;
					ch = input.nextChar();
					currentLine.add(ch);
				} else {
					processQuoteEscape();
					prev = ch;
					ch = input.nextChar();
					currentLine.add(ch);

					if (unescaped && (ch == newLine || matchDelimiter())) {
						String previousLine = input.previousLine();
						System.out.println("Line parsed in CsvParser (978): " + currentLine.toString() +
								" -- prev. line from input: " + previousLine);
						currentLine.clear();

						return ParseRecordResult.CONTINUE_PARSING;
					}
				}
			}
		}

		// handles whitespaces after quoted value: whitespaces are ignored. Content after whitespaces may be parsed if 'parseUnescapedQuotes' is enabled.
		if (ch != newLine && ch <= ' ' && whitespaceRangeStart < ch && !matchDelimiterAfterQuote()) {
			whitespaceAppender.reset();
			do {
				//saves whitespaces after value
				whitespaceAppender.append(ch);
				ch = input.nextChar();
				currentLine.add(ch);

				//found a new line, go to next record.
				if (ch == newLine) {
					if (keepQuotes) {
						output.appender.append(quote);
						currentLine.add(quote);
					}

					String previousLine = input.previousLine();
					System.out.println("Line parsed in CsvParser (1005): " + currentLine.toString() +
							" -- prev. line from input: " + previousLine);
					currentLine.clear();

					return ParseRecordResult.CONTINUE_PARSING;
				}
				if (matchDelimiterAfterQuote()) {
					return ParseRecordResult.CONTINUE_PARSING;
				}
			} while (ch <= ' ' && whitespaceRangeStart < ch);

			//there's more stuff after the quoted value, not only empty spaces.
			if (parseUnescapedQuotes && !matchDelimiterAfterQuote()) {
				if (output.appender instanceof DefaultCharAppender) {
					//puts the quote before whitespaces back, then restores the whitespaces
					output.appender.append(quote);
					currentLine.add(quote);

					((DefaultCharAppender) output.appender).append(whitespaceAppender);
				}
				//the next character is not the escape character, put it there
				if (parseUnescapedQuotesUntilDelimiter || ch != quote && ch != quoteEscape) {
					output.appender.append(ch);
					currentLine.add(ch);
				}

				//sets this character as the previous character (may be escaping)
				//calls recursively to keep parsing potentially quoted content
				prev = ch;
				return parseQuotedValue();
			} else if (keepQuotes) {
				output.appender.append(quote);
				currentLine.add(quote);
			}
		} else if (keepQuotes && (!unescaped || quoteHandling == STOP_AT_CLOSING_QUOTE)) {
			output.appender.append(quote);
			currentLine.add(quote);
		}

		return ParseRecordResult.CONTINUE_PARSING;
	}

	private void parseValueProcessingEscapeMultiDelimiter() {
		while (ch != newLine && !matchDelimiter()) {
			if (ch != quote && ch != quoteEscape) {
				if (prev == quote) { //unescaped quote detected
					handleUnescapedQuoteInValue();
					return;
				}
				output.appender.append(ch);
				currentLine.add(ch);
			} else {
				processQuoteEscape();
			}
			prev = ch;
			ch = input.nextChar();
			currentLine.add(ch);
		}

		String previousLine = input.previousLine();
		System.out.println("Line parsed in CsvParser (782): " + currentLine.toString() +
				" -- prev. line from input: " + previousLine);
		currentLine.clear();
	}

	public void printParsingStats() {
		System.out.println("================ PARSING STATS ================");
		System.out.println("Total lines parsed: " + input.lineCount());
		System.out.println("Distinct Potential CSV record counts encountered: " + csvPotentialRecordLines.keySet().size());
		System.out.println("Distinct Potential CSV records count: ");
		for (Long key : csvPotentialRecordLines.keySet()) {
			Map<Long, String> records = csvPotentialRecordLines.get(key);
			System.out.println(" * Potential value count " + key + ": " + records.size() + " lines.");
			for (Long lineKey: records.keySet()) {
				System.out.println("    - Line " + lineKey + ": " + records.get(lineKey));
			}
		}

		System.out.println("Distinct Parsed CSV record counts encountered: " + csvPotentialRecordLines.keySet().size());
		System.out.println("Distinct Parsed CSV records by separator count: ");
		for (Long key : csvParsedRecordLines.keySet()) {
			Map<Long, String> records = csvParsedRecordLines.get(key);
			System.out.println(" * Parsed value count " + key + ": " + records.size() + " lines.");
			for (Long lineKey: records.keySet()) {
				System.out.println("    - Line " + lineKey + ": " + records.get(lineKey));
			}
		}
	}
}
