package com.alibaba.druid.sql.dialect.mysql.parser;

import static com.alibaba.druid.sql.parser.CharTypes.isFirstIdentifierChar;
import static com.alibaba.druid.sql.parser.CharTypes.isIdentifierChar;
import static com.alibaba.druid.sql.parser.LayoutCharacters.EOI;
import static com.alibaba.druid.sql.parser.Token.LITERAL_CHARS;

import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.SQLParseException;
import com.alibaba.druid.sql.parser.Token;

public class MySqlLexer extends Lexer {

	public MySqlLexer(char[] input, int inputLength) {
		super(input, inputLength);
	}

	public MySqlLexer(String input) {
		super(input);
	}

	public void scanIdentifier() { // 扫描标识符
		final char first = ch;

		if (ch == '`') { // 如果字符为 `
			int hash = first;

			offsetCache = curIndex;
			sizeCache = 1;
			char ch;
			for (;;) {
				ch = sql[++curIndex];

				if (ch == '`') {
					sizeCache++;
					ch = sql[++curIndex];
					break;
				} else if (ch == EOI) {
					throw new SQLParseException("illegal identifier");
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
		} else { // 如果为字符 循环取出字符 直到字符是空白

			final boolean firstFlag = isFirstIdentifierChar(first);
			if (!firstFlag) {
				throw new SQLParseException("illegal identifier");
			}

			int hash = first;

			offsetCache = curIndex;
			sizeCache = 1;
			char ch;
			for (;;) {
				ch = sql[++curIndex]; // 向前获取一个字符 会超前取出一个字符

				if (!isIdentifierChar(ch)) {
					break; // 扫描到不是字符则退出
				}

				hash = 31 * hash + ch; // 字符 hash

				sizeCache++; // 缓存大小加1
				continue;
			}

			this.ch = sql[curIndex]; // 取出之前扫描的字符

			stringVal = symbolTable.addSymbol(sql, offsetCache, sizeCache, hash); // 字符缓存容器 返回一整串的字符
			Token tok = keywods.getKeyword(stringVal); // 从关键字常量当中返回这个字符串的token
			if (tok != null) {
				token = tok;
			} else {
				token = Token.IDENTIFIER; // 不是特点的关键字就标识为标识符 IDENTIFIER
			}
		}
	}

	protected void scanString() {
		offsetCache = curIndex;
		boolean hasSpecial = false; // 有特殊处理标记

		for (;;) {
			if (curIndex >= sqlLength) {
				lexError(tokenPos, "unclosed.str.lit");
				return;
			}

			ch = sql[++curIndex]; // 先向前扫

			if (ch == '\\') {
				scanChar();
				if (!hasSpecial) {
					System.arraycopy(sql, offsetCache + 1, sbuf, 0, sizeCache);
					hasSpecial = true;
				}

				switch (ch) {
				case '\0':
					putChar('\0');
					break;
				case '\'':
					putChar('\'');
					break;
				case '"':
					putChar('"');
					break;
				case 'b':
					putChar('\b');
					break;
				case 'n':
					putChar('\n');
					break;
				case 'r':
					putChar('\r');
					break;
				case 't':
					putChar('\t');
					break;
				case '\\':
					putChar('\\');
					break;
				case 'Z':
					putChar((char) 0x1A); // ctrl + Z
					break;
				default:
					putChar(ch);
					break;
				}
				scanChar();
			}

			if (ch == '\'') { // 为 \ 其实就是' 字符
				scanChar(); // 向前扫一个字符
				if (ch != '\'') { // 如果不等于 ' 字符
					token = LITERAL_CHARS;  // 标记该token为 文字字符
					break;
				} else {
					System.arraycopy(sql, offsetCache + 1, sbuf, 0, sizeCache);
					hasSpecial = true;
					putChar('\'');
					continue;
				}
			}

			if (!hasSpecial) {
				sizeCache++; // 判断下一个字符 但没被case到加1了
				continue;
			}

			if (sizeCache == sbuf.length) {
				putChar(ch);
			} else {
				sbuf[sizeCache++] = ch;
			}
		}

		if (!hasSpecial) {
			stringVal = new String(sql, offsetCache + 1, sizeCache); // 取出 ', '里的 , 字符
		} else {
			stringVal = new String(sbuf, 0, sizeCache);
		}
	}
}
