package com.alibaba.druid.sql.parser;

import static com.alibaba.druid.sql.parser.CharTypes.isFirstIdentifierChar;
import static com.alibaba.druid.sql.parser.CharTypes.isIdentifierChar;
import static com.alibaba.druid.sql.parser.CharTypes.isWhitespace;
import static com.alibaba.druid.sql.parser.LayoutCharacters.EOI;
import static com.alibaba.druid.sql.parser.Token.COLON;
import static com.alibaba.druid.sql.parser.Token.COLONEQ;
import static com.alibaba.druid.sql.parser.Token.COMMA;
import static com.alibaba.druid.sql.parser.Token.EOF;
import static com.alibaba.druid.sql.parser.Token.ERROR;
import static com.alibaba.druid.sql.parser.Token.LBRACE;
import static com.alibaba.druid.sql.parser.Token.LBRACKET;
import static com.alibaba.druid.sql.parser.Token.LITERAL_ALIAS;
import static com.alibaba.druid.sql.parser.Token.LITERAL_CHARS;
import static com.alibaba.druid.sql.parser.Token.LPAREN;
import static com.alibaba.druid.sql.parser.Token.RBRACE;
import static com.alibaba.druid.sql.parser.Token.RBRACKET;
import static com.alibaba.druid.sql.parser.Token.RPAREN;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @author shaojin.wensj
 */
public class Lexer {
	protected final char[] sql; // SQL 缓存区
	protected int curIndex; // token之后下一个索引
	protected int sqlLength; // SQL长度
	// QS_TODO what is the purpose?
	protected int eofPos; // 字符结束标志长度

	/** The current character. */
	protected char ch;

	/** The token's position, 0-based offset from beginning of text. */
	protected int tokenPos;

	/** A character buffer for literals. */
	protected final static ThreadLocal<char[]> sbufRef = new ThreadLocal<char[]>();
	protected char[] sbuf;

	/** string point as size */
	protected int sizeCache; // 根据扫描到的字符自增多少
	/** string point as offset */
	protected int offsetCache; // Token 扫描到的缓存节点起点索引

	protected SymbolTable symbolTable = new SymbolTable();

	/**
	 * The token, set by nextToken().
	 */
	protected Token token;

	protected Keywords keywods = Keywords.DEFAULT_KEYWORDS;

	protected String stringVal;

	public Lexer(String input) {
		this(input.toCharArray(), input.length());
	}

	public Lexer(char[] input, int inputLength) {
		this.sbuf = sbufRef.get(); // new char[1024]; 从缓冲区当中获取
		if (this.sbuf == null) {
			this.sbuf = new char[1024]; // 缓冲器没有对象 重新获取
			sbufRef.set(sbuf); // 重新set
		}

		this.eofPos = inputLength; // 字符结束标志长度

		// QS_TODO ?
		if (inputLength == input.length) { // 输入长度 跟现在长度相等
			if (input.length > 0 && isWhitespace(input[input.length - 1])) { // 判断是不是空白字符
				inputLength--; // 空白字符长度就减一
			} else {
				char[] newInput = new char[inputLength + 1]; // 长度加+1个字节
				System.arraycopy(input, 0, newInput, 0, input.length); // 数组复制到 newInput
				input = newInput; // 新的数组重新赋值给 input
			}
		}
		this.sql = input;
		this.sqlLength = inputLength;
		this.sql[this.sqlLength] = EOI; // 82 赋值为结束标志位 0x1A
		this.curIndex = -1; // 当前索引

		scanChar(); // 扫描第一个字符
	}
	// 向前扫描取出一个字符
	protected final void scanChar() {
		ch = sql[++curIndex];
	}

	/**
	 * Report an error at the given position using the provided arguments.
	 */
	protected void lexError(int pos, String key, Object... args) {
		token = ERROR;
	}

	/**
	 * Report an error at the current token position using the provided arguments.
	 */
	private void lexError(String key, Object... args) {
		lexError(tokenPos, key, args);
	}

	/**
	 * Return the current token, set by nextToken().
	 */
	public final Token token() { // 获取当前token
		return token;
	}
	// 扫描数字  空白 字符串 先扫描下一个标识符 再把当前 token 标识为 plus 解析完 token（遇到accept再次扫描token） 之后扫描下一个 token
	public final void nextToken() {
		sizeCache = 0;

		for (;;) {
			tokenPos = curIndex;
			// ch 是下一个字符的
			if (isWhitespace(ch)) { // 如果是空白的字符 继续获取下一个字符
				scanChar();
				continue;
			}// QS_TODO skip comment

			// QS_TODO id may start from digit
			if (isFirstIdentifierChar(ch)) { // 判断第一个字符是否为字符类型 比如,不是我们预设的字符
				if (ch == 'N') {
					if (sql[curIndex + 1] == '\'') {
						++curIndex;
						ch = '\'';
						scanString();
						token = Token.LITERAL_NCHARS;
						return;
					}
				}

				scanIdentifier(); // 扫描一整串的标识符
				return;
			}

			switch (ch) { // 不是特定的字符 是数字 比如 1+1
			case '0':
				if (sql[curIndex + 1] == 'x') {
					scanChar();
					scanChar();
					scanHexaDecimal();
				} else {
					scanNumber();
				}
				return;
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				scanNumber(); // 扫描数字
				return; // 扫描完数字返回
			case ',': // 扫描 ,
				scanChar(); // 先扫描下一个
				token = COMMA; // 再取 逗号, token
				return;
			case '(': // 扫描 ( 比如一些函数中的括号啊
				scanChar();
				token = LPAREN; // 返回 左边 括号 token 标识符
				return;
			case ')':
				scanChar();
				token = RPAREN; // 返回 右边括号 token 标识符
				return;
			case '[':
				scanChar();
				token = LBRACKET;
				return;
			case ']':
				scanChar();
				token = RBRACKET;
				return;
			case '{':
				scanChar();
				token = LBRACE;
				return;
			case '}':
				scanChar();
				token = RBRACE;
				return;
			case ':':
				scanChar();
				if (ch == '=') {
					scanChar();
					token = COLONEQ;
				} else {
					token = COLON;
				}
				return;
			case '.':
				scanChar();
				token = Token.DOT;
				return;
			case '\'': // 一般是 \' 转义
				scanString(); // 扫描字符串
				return;
			case '\"':
				scanAlias();
				return;
			case '*':
				scanChar();
				token = Token.STAR;
				return;
			case '?':
				scanChar();
				token = Token.QUES;
				return;
			case ';': // 如果为;
				scanChar();
				token = Token.SEMI; // 标记当前 token 为 ;
				return;
			case '`':
				throw new SQLParseException("TODO"); // TODO
			case '@':
				scanVariable();
				token = Token.USR_VAR;
				return;
			default:
				if (Character.isLetter(ch)) { // 如果是字母
					scanIdentifier();
					return;
				}

				if (isOperator(ch)) { // 如果是 操作符 比如 +
					scanOperator(); // 扫描操作符
					return;
				}

				// QS_TODO ?
				if (curIndex == sqlLength || ch == EOI && curIndex + 1 == sqlLength) { // JLS
					token = EOF;
					tokenPos = curIndex = eofPos;
				} else {
					lexError("illegal.char", String.valueOf((int) ch));
					scanChar();
				}

				return;
			}
		}

	}

	private final void scanOperator() {
		switch (ch) {
		case '+': // 如果为 + 操作符
			scanChar(); //
			token = Token.PLUS; // 先扫描下一个标识符 再把当前 token 标识为 plus 解析完 token 之后扫描下一个 token
			break;
		case '-':
			scanChar();
			token = Token.SUB;
			break;
		case '*':
			scanChar();
			token = Token.STAR;
			break;
		case '/':
			scanChar();
			token = Token.SLASH;
			break;
		case '&':
			scanChar();
			if (ch == '&') {
				scanChar();
				token = Token.AMPAMP;
			} else {
				token = Token.AMP;
			}
			break;
		case '|':
			scanChar();
			if (ch == '|') {
				scanChar();
				token = Token.BARBAR;
			} else {
				token = Token.BAR;
			}
			break;
		case '^':
			scanChar();
			token = Token.CARET;
			break;
		case '%':
			scanChar();
			token = Token.PERCENT;
			break;
		case '=':
			scanChar();
			if (ch == '=') {
				scanChar();
				token = Token.EQEQ;
			} else {
				token = Token.EQ;
			}
			break;
		case '>':
			scanChar();
			if (ch == '=') {
				scanChar();
				token = Token.GTEQ;
			} else if (ch == '>') {
				scanChar();
				token = Token.GTGT;
			} else {
				token = Token.GT;
			}
			break;
		case '<':
			scanChar();
			if (ch == '=') {
				scanChar();
				if (ch == '>') {
					token = Token.LTEQGT;
					scanChar();
				} else {
					token = Token.LTEQ;
				}
			} else if (ch == '>') {
				scanChar();
				token = Token.LTGT;
			} else if (ch == '<') {
				scanChar();
				token = Token.LTLT;
			} else {
				token = Token.LT;
			}
			break;
		case '!':
			scanChar();
			if (ch == '=') {
				scanChar();
				token = Token.BANGEQ;
			} else if (ch == '>') {
				scanChar();
				token = Token.BANGGT;
			} else if (ch == '<') {
				scanChar();
				token = Token.BANGLT;
			} else {
				token = Token.BANG;
			}
			break;
		case '?':
			scanChar();
			token = Token.QUES;
			break;
		case '~':
			scanChar();
			token = Token.TILDE;
			break;
		default:
			throw new SQLParseException("TODO");
		}
	}

	protected void scanString() {
		offsetCache = curIndex;
		boolean hasSpecial = false;

		for (;;) {
			if (curIndex >= sqlLength) {
				lexError(tokenPos, "unclosed.str.lit");
				return;
			}

			ch = sql[++curIndex];

			if (ch == '\'') {
				scanChar();
				if (ch != '\'') {
					token = LITERAL_CHARS;
					break;
				} else {
					System.arraycopy(sql, offsetCache + 1, sbuf, 0, sizeCache);
					hasSpecial = true;
					putChar('\'');
					continue;
				}
			}

			if (!hasSpecial) {
				sizeCache++;
				continue;
			}

			if (sizeCache == sbuf.length) {
				putChar(ch);
			} else {
				sbuf[sizeCache++] = ch;
			}
		}

		if (!hasSpecial) {
			stringVal = new String(sql, offsetCache + 1, sizeCache);
		} else {
			stringVal = new String(sbuf, 0, sizeCache);
		}
	}

	private final void scanAlias() {
		for (;;) {
			if (curIndex >= sqlLength) {
				lexError(tokenPos, "unclosed.str.lit");
				return;
			}

			ch = sql[++curIndex];

			if (ch == '\"') {
				scanChar();
				token = LITERAL_ALIAS;
				return;
			}

			if (sizeCache == sbuf.length) {
				putChar(ch);
			} else {
				sbuf[sizeCache++] = ch;
			}
		}
	}

	public void scanVariable() {
		final char first = ch;

		if (ch != '@' && ch != ':') {
			throw new SQLParseException("illegal variable");
		}

		int hash = first;

		offsetCache = curIndex;
		sizeCache = 1;
		char ch;
		for (;;) {
			ch = sql[++curIndex];

			if (!isIdentifierChar(ch)) {
				break;
			}

			hash = 31 * hash + ch;

			sizeCache++;
			continue;
		}

		this.ch = sql[curIndex];

		stringVal = symbolTable.addSymbol(sql, offsetCache, sizeCache, hash);
		Token tok = keywods.getKeyword(stringVal);
		if (tok != null) {
			token = tok;
		} else {
			token = Token.IDENTIFIER;
		}
	}

	public void scanIdentifier() {
		final char first = ch;

		final boolean firstFlag = isFirstIdentifierChar(first);
		if (!firstFlag) {
			throw new SQLParseException("illegal identifier");
		}

		int hash = first;

		offsetCache = curIndex;
		sizeCache = 1;
		char ch;
		for (;;) {
			ch = sql[++curIndex];

			if (!isIdentifierChar(ch)) { // 判断是否是字符 是字符继续 不是就退出
				break;
			}

			hash = 31 * hash + ch;

			sizeCache++;
			continue;
		}

		this.ch = sql[curIndex];

		stringVal = symbolTable.addSymbol(sql, offsetCache, sizeCache, hash);
		Token tok = keywods.getKeyword(stringVal);
		if (tok != null) {
			token = tok;
		} else {
			token = Token.IDENTIFIER;
		}
	}

	public void scanNumber() { // 扫描到为特定的字符就取下一个 进而判断是 浮点还是数字
		offsetCache = curIndex;

		if (ch == '-') { // 如果当前字符等于 -
			sizeCache++;
			ch = sql[++curIndex];
		}

		for (;;) { // 循环扫描数字
			if (ch >= '0' && ch <= '9') { // 当前字符为 0 ~ 9
				sizeCache++; // 大小缓存
			} else {
				break;
			}
			ch = sql[++curIndex]; // 取新的字符
		}

		boolean isDouble = false;

		if (ch == '.') { // 如果字符为 . 为浮点型
			sizeCache++;
			ch = sql[++curIndex];
			isDouble = true;

			for (;;) {
				if (ch >= '0' && ch <= '9') {
					sizeCache++;
				} else {
					break;
				}
				ch = sql[++curIndex];
			}
		}

		if (ch == 'e' || ch == 'E') {
			sizeCache++;
			ch = sql[++curIndex];

			if (ch == '+' || ch == '-') {
				sizeCache++;
				ch = sql[++curIndex];
			}

			for (;;) {
				if (ch >= '0' && ch <= '9') {
					sizeCache++;
				} else {
					break;
				}
				ch = sql[++curIndex];
			}

			isDouble = true;
		}

		if (isDouble) { // 根据下一个字符判断为 浮点型还是 数字
			token = Token.LITERAL_NUM_MIX_DIGIT;
		} else {
			token = Token.LITERAL_NUM_PURE_DIGIT; // 该 token 由数字组成
		}
	}

	public void scanHexaDecimal() {
		offsetCache = curIndex;

		if (ch == '-') {
			sizeCache++;
			ch = sql[++curIndex];
		}

		for (;;) {
			if (CharTypes.isHex(ch)) {
				sizeCache++;
			} else {
				break;
			}
			ch = sql[++curIndex];
		}

		token = Token.LITERAL_HEX;
	}

	public String hexString() throws NumberFormatException {
		return new String(sql, offsetCache, sizeCache);
	}

	public final boolean isDigit(char ch) {
		return ch >= '0' && ch <= '9';
	}

	/**
	 * Append a character to sbuf.
	 */
	protected final void putChar(char ch) {
		if (sizeCache == sbuf.length) {
			char[] newsbuf = new char[sbuf.length * 2];
			System.arraycopy(sbuf, 0, newsbuf, 0, sbuf.length);
			sbuf = newsbuf;
		}
		sbuf[sizeCache++] = ch;
	}

	/**
	 * Return the current token's position: a 0-based offset from beginning of the raw input stream (before unicode translation)
	 */
	public final int pos() {
		return tokenPos;
	}

	/**
	 * The value of a literal token, recorded as a string. For integers, leading 0x and 'l' suffixes are suppressed.
	 */
	public final String stringVal() { // 获取 token 的值
		return stringVal;
	}
	// 扫描是否为操作符 如果为操作符 比如 + 则 返回 true
	private boolean isOperator(char ch) {
		switch (ch) {
		case '!':
		case '%':
		case '&':
		case '*':
		case '+': // 为+字符则为 true
		case '-':
		case '<':
		case '=':
		case '>':
		case '^':
		case '|':
		case '~':
		case '/':
		case ';':
			return true;
		default:
			return false;
		}
	}

	private static final long MULTMIN_RADIX_TEN = Long.MIN_VALUE / 10;
	private static final long N_MULTMAX_RADIX_TEN = -Long.MAX_VALUE / 10;

	private final static int[] digits = new int[(int) '9' + 1];

	static {
		for (int i = '0'; i <= '9'; ++i) {
			digits[i] = i - '0';
		}
	}

	// QS_TODO negative number is invisible for lexer
	public Number integerValue() throws NumberFormatException { // 获取数字的值
		long result = 0;
		boolean negative = false;
		int i = offsetCache, max = offsetCache + sizeCache; // sizeCache 取出字符的缓存数
		long limit;
		long multmin;
		int digit;

		if (sql[offsetCache] == '-') { // 如果要取的缓存当前字符为 -
			negative = true;
			limit = Long.MIN_VALUE;
			i++;
		} else {
			limit = -Long.MAX_VALUE; // 负数最大值
		}
		multmin = negative ? MULTMIN_RADIX_TEN : N_MULTMAX_RADIX_TEN; // 负数最大值 / 10
		if (i < max) {
			digit = digits[sql[i++]]; // 取结果
			result = -digit; // 取负数
		}
		while (i < max) {
			// Accumulating negatively avoids surprises near MAX_VALUE
			digit = digits[sql[i++]];
			if (result < multmin) {
				return new BigInteger(numberString());
			}
			result *= 10;
			if (result < limit + digit) {
				return new BigInteger(numberString());
			}
			result -= digit;
		}

		if (negative) {
			if (i > offsetCache + 1) {
				if (result >= Integer.MIN_VALUE) {
					return (int) result;
				}
				return result;
			} else { /* Only got "-" */
				throw new NumberFormatException(numberString());
			}
		} else {
			result = -result; // 负负得正
			if (result <= Integer.MAX_VALUE) {
				return (int) result; // 小于 int 最大值 返回
			}
			return result;
		}
	}

	public final String numberString() {
		return new String(sql, offsetCache, sizeCache);
	}

	public BigDecimal decimalValue() {
		return new BigDecimal(sql, offsetCache, sizeCache);
	}
}
