use crate::errors::{Error, Result};
use std::{iter::Peekable, str::Chars};

/// 定义token
#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    /// 关键字
    Keyword(Keyword),
    /// 这里只做词法分析，因此这里number依旧存string
    Number(String),
    /// 字符串
    String(String),
    /// 标识符，表名 字符名 函数
    Ident(String),
    // 下面是操作符号
    /// .
    Period,
    /// =
    Equal,
    /// >
    GreaterThan,
    /// >=
    GreaterThanOrEqual,
    /// <
    LessThan,
    /// <=
    LessThanOrEqual,
    /// <>
    LessOrGreaterThan,
    /// +
    Plus,
    /// -
    Minus,
    /// *
    Asterisk,
    /// /
    Slash,
    /// ^
    Caret,
    /// %
    Percent,
    /// !
    Exclamation,
    /// !=
    NotEqual,
    /// ?
    Question,
    /// (
    OpenParen,
    /// )
    CloseParen,
    /// ,
    Comma,
    /// ;
    Semicolon,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Token::Number(n) => n,
            Token::String(s) => s,
            Token::Ident(s) => s,
            Token::Keyword(k) => k.to_str(),
            Token::Period => ".",
            Token::Equal => "=",
            Token::GreaterThan => ">",
            Token::GreaterThanOrEqual => ">=",
            Token::LessThan => "<",
            Token::LessThanOrEqual => "<=",
            Token::LessOrGreaterThan => "<>",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Asterisk => "*",
            Token::Slash => "/",
            Token::Caret => "^",
            Token::Percent => "%",
            Token::Exclamation => "!",
            Token::NotEqual => "!=",
            Token::Question => "?",
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
        })
    }
}

/// 词法分析器的关键字，按照首字母排序
#[derive(Clone, Debug, PartialEq)]
pub enum Keyword {
    And,
    As,
    Asc,
    Begin,
    Bool,
    Boolean,
    By,
    Char,
    Commit,
    Create,
    Cross,
    Default,
    Delete,
    Desc,
    Double,
    Drop,
    Explain,
    False,
    Float,
    From,
    Group,
    Having,
    Index,
    Infinity,
    Inner,
    Insert,
    Int,
    Integer,
    Into,
    Is,
    Join,
    Key,
    Left,
    Like,
    Limit,
    NaN,
    Not,
    Null,
    Of,
    Offset,
    On,
    Only,
    Or,
    Order,
    Outer,
    Primary,
    Read,
    References,
    Right,
    Rollback,
    Select,
    Set,
    String,
    System,
    Table,
    Text,
    Time,
    Transaction,
    True,
    Unique,
    Update,
    Values,
    Varchar,
    Where,
    Write,
}

impl Keyword {
    /// 通过string变成Keyword, 如果不匹配返回null 记得全部大写匹配
    fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "AS" => Some(Self::As),
            "ASC" => Some(Self::Asc),
            "AND" => Some(Self::And),
            "BEGIN" => Some(Self::Begin),
            "BOOL" => Some(Self::Bool),
            "BOOLEAN" => Some(Self::Boolean),
            "BY" => Some(Self::By),
            "CHAR" => Some(Self::Char),
            "COMMIT" => Some(Self::Commit),
            "CREATE" => Some(Self::Create),
            "CROSS" => Some(Self::Cross),
            "DEFAULT" => Some(Self::Default),
            "DELETE" => Some(Self::Delete),
            "DESC" => Some(Self::Desc),
            "DOUBLE" => Some(Self::Double),
            "DROP" => Some(Self::Drop),
            "EXPLAIN" => Some(Self::Explain),
            "FALSE" => Some(Self::False),
            "FLOAT" => Some(Self::Float),
            "FROM" => Some(Self::From),
            "GROUP" => Some(Self::Group),
            "HAVING" => Some(Self::Having),
            "INDEX" => Some(Self::Index),
            "INFINITY" => Some(Self::Infinity),
            "INNER" => Some(Self::Inner),
            "INSERT" => Some(Self::Insert),
            "INT" => Some(Self::Int),
            "INTEGER" => Some(Self::Integer),
            "INTO" => Some(Self::Into),
            "IS" => Some(Self::Is),
            "JOIN" => Some(Self::Join),
            "KEY" => Some(Self::Key),
            "LEFT" => Some(Self::Left),
            "LIKE" => Some(Self::Like),
            "LIMIT" => Some(Self::Limit),
            "NAN" => Some(Self::NaN),
            "NOT" => Some(Self::Not),
            "NULL" => Some(Self::Null),
            "OF" => Some(Self::Of),
            "OFFSET" => Some(Self::Offset),
            "ON" => Some(Self::On),
            "ONLY" => Some(Self::Only),
            "OR" => Some(Self::Or),
            "ORDER" => Some(Self::Order),
            "OUTER" => Some(Self::Outer),
            "PRIMARY" => Some(Self::Primary),
            "READ" => Some(Self::Read),
            "REFERENCES" => Some(Self::References),
            "RIGHT" => Some(Self::Right),
            "ROLLBACK" => Some(Self::Rollback),
            "SELECT" => Some(Self::Select),
            "SET" => Some(Self::Set),
            "STRING" => Some(Self::String),
            "SYSTEM" => Some(Self::System),
            "TABLE" => Some(Self::Table),
            "TEXT" => Some(Self::Text),
            "TIME" => Some(Self::Time),
            "TRANSACTION" => Some(Self::Transaction),
            "TRUE" => Some(Self::True),
            "UNIQUE" => Some(Self::Unique),
            "UPDATE" => Some(Self::Update),
            "VALUES" => Some(Self::Values),
            "VARCHAR" => Some(Self::Varchar),
            "WHERE" => Some(Self::Where),
            "WRITE" => Some(Self::Write),
            _ => None,
        }
    }
    /// 将自己转换为string
    fn to_str(&self) -> &str {
        match self {
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::And => "AND",
            Self::Begin => "BEGIN",
            Self::Bool => "BOOL",
            Self::Boolean => "BOOLEAN",
            Self::By => "BY",
            Self::Char => "CHAR",
            Self::Commit => "COMMIT",
            Self::Create => "CREATE",
            Self::Cross => "CROSS",
            Self::Default => "DEFAULT",
            Self::Delete => "DELETE",
            Self::Desc => "DESC",
            Self::Double => "DOUBLE",
            Self::Drop => "DROP",
            Self::Explain => "EXPLAIN",
            Self::False => "FALSE",
            Self::Float => "FLOAT",
            Self::From => "FROM",
            Self::Group => "GROUP",
            Self::Having => "HAVING",
            Self::Index => "INDEX",
            Self::Infinity => "INFINITY",
            Self::Inner => "INNER",
            Self::Insert => "INSERT",
            Self::Int => "INT",
            Self::Integer => "INTEGER",
            Self::Into => "INTO",
            Self::Is => "IS",
            Self::Join => "JOIN",
            Self::Key => "KEY",
            Self::Left => "LEFT",
            Self::Like => "LIKE",
            Self::Limit => "LIMIT",
            Self::NaN => "NAN",
            Self::Not => "NOT",
            Self::Null => "NULL",
            Self::Of => "OF",
            Self::Offset => "OFFSET",
            Self::On => "ON",
            Self::Only => "ONLY",
            Self::Outer => "OUTER",
            Self::Or => "OR",
            Self::Order => "ORDER",
            Self::Primary => "PRIMARY",
            Self::Read => "READ",
            Self::References => "REFERENCES",
            Self::Right => "RIGHT",
            Self::Rollback => "ROLLBACK",
            Self::Select => "SELECT",
            Self::Set => "SET",
            Self::String => "STRING",
            Self::System => "SYSTEM",
            Self::Table => "TABLE",
            Self::Text => "TEXT",
            Self::Time => "TIME",
            Self::Transaction => "TRANSACTION",
            Self::True => "TRUE",
            Self::Unique => "UNIQUE",
            Self::Update => "UPDATE",
            Self::Values => "VALUES",
            Self::Varchar => "VARCHAR",
            Self::Where => "WHERE",
            Self::Write => "WRITE",
        }
    }
}

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

impl From<Keyword> for Token {
    fn from(value: Keyword) -> Self {
        Self::Keyword(value)
    }
}

pub struct Laxer<'a> {
    iter: Peekable<Chars<'a>>,
}

impl<'a> Laxer<'a> {
    pub fn new(input: &'a str) -> Self {
        let mut iter = input.chars().peekable();
        Self { iter }
    }

    pub fn get_next(&mut self) -> Result<Option<Token>> {
        // 将空格排除
        self.term();
        match self.iter.peek() {
            // indent
            Some('`') => self.get_ident_with_backtick(),
            Some(c) if c.is_alphabetic() => self.get_ident(),
            // string
            Some('\"') => self.get_string(),
            // number
            Some(c) if c.is_digit(10) => self.get_number(),
            // 都不是的话看看是不是一些符号
            Some(_) => self.get_symbol(),
            None => Ok(None),
        }
    }
    /// 解析特殊符号 注意两个符号连接的情况 例如 >= <= !=
    fn get_symbol(&mut self) -> Result<Option<Token>> {
        let r = self
            .next_judge(|&&c| match c {
                '.' | '=' | '>' | '<' | '+' | '-' | '*' | '/' | '^' | '%' | '!' | '?' | '('
                | ')' | ',' | 'l' | ';' => true,
                _ => false,
            })
            .map(|c| -> Token {
                match c {
                    '.' => Token::Period,
                    '=' => Token::Equal,
                    '>' => {
                        if self.next_char_expect('=').is_some() {
                            Token::GreaterThanOrEqual
                        } else {
                            Token::GreaterThan
                        }
                    }
                    '<' => {
                        if self.next_char_expect('=').is_some() {
                            Token::LessThanOrEqual
                        } else if self.next_char_expect('>').is_some() {
                            Token::LessOrGreaterThan
                        } else {
                            Token::LessThan
                        }
                    }
                    '+' => Token::Plus,
                    '-' => Token::Minus,
                    '*' => Token::Asterisk,
                    '/' => Token::Slash,
                    '^' => Token::Caret,
                    '%' => Token::Percent,
                    '!' => {
                        if self.next_char_expect('=').is_some() {
                            Token::NotEqual
                        } else {
                            Token::Exclamation
                        }
                    }
                    '?' => Token::Question,
                    '(' => Token::OpenParen,
                    ')' => Token::CloseParen,
                    ',' => Token::Comma,
                    ';' => Token::Semicolon,
                    // 前面filter过了 不可能到这里
                    _ => Token::Plus,
                }
            });
        Ok(r)
    }

    /// 获得number 这里就不算e什么什么的了，注意一下小数点就行 不需要考虑负号
    /// 负号相当于一个数学前缀运算符
    fn get_number(&mut self) -> Result<Option<Token>> {
        let mut res = String::new();

        while self.peek_judge(|c| c.is_digit(10)) {
            res.push(self.iter.next().unwrap());
        }
        if res.len() == 0 {
            return Err(Error::Parse(
                "parse number need have a number at first".to_string(),
            ));
        }

        if let Some(sep) = self.next_judge(|c| **c == '.') {
            res.push(sep);
            while self.peek_judge(|c| c.is_digit(10)) {
                res.push(self.iter.next().unwrap());
            }
        }
        Ok(Some(Token::Number(res)))
    }

    /// 获得被双引号包裹的string
    fn get_string(&mut self) -> Result<Option<Token>> {
        match self.next_char_expect('\"') {
            Some(_) => {}
            None => {
                return Err(Error::Parse("expect get \" at first".to_string()));
            }
        };

        let mut res = String::new();
        while self.peek_judge(|c| **c != '\"') {
            res.push(self.iter.next().unwrap());
        }

        match self.next_char_expect('\"') {
            Some(_) => {}
            None => {
                return Err(Error::Parse(
                    "expect get \" in the end of string".to_string(),
                ));
            }
        };

        Ok(Some(Token::String(res)))
    }

    /// ident 开头必须是字母 后续才可以是 _ , 数字 其余报错
    /// 直接获得ident 注意这里需要查看一下是否有关键字
    fn get_ident(&mut self) -> Result<Option<Token>> {
        let mut res = String::new();

        // 开头必须是字母
        let n = self.next_judge(|c| c.is_alphabetic());
        match n {
            Some(c) => res.push(c),
            None => {
                return Err(Error::Parse("expact alphabetic at first".to_string()));
            }
        }
        while let Some(c) = self.next_judge(|c| c.is_alphanumeric() || **c == '_') {
            res.push(c);
        }
        Token::Keyword(Keyword::As);
        Ok(Keyword::from_str(&res)
            .map(|c| Token::Keyword(c))
            .or_else(|| Some(Token::Ident(res.to_lowercase()))))
    }

    /// 获得反引号包围的ident
    fn get_ident_with_backtick(&mut self) -> Result<Option<Token>> {
        if self.next_char_expect('`').is_none() {
            return Err(crate::errors::Error::Parse(format!("expact \"`\" !")));
        }
        let mut res = String::new();
        // 考虑到只有一个 ` 的可能， 如果只有一个` 就会迭代到None
        let n = self.next_judge(|c| c.is_alphabetic());
        match n {
            Some(c) => res.push(c),
            None => {
                return Err(Error::Parse("expact alphabetic at first".to_string()));
            }
        }
        while let Some(c) = self.next_judge(|c| c.is_alphabetic() || **c == '_') {
            res.push(c);
        }
        // 迭代到None就需要返回错误
        if self.next_char_expect('`').is_none() {
            return Err(crate::errors::Error::Parse(format!("expact \"`\" !")));
        }
        Ok(Some(Token::Ident(res.to_lowercase())))
    }

    /// fn是判断,如果符合就返回字符, 迭代器往下一个
    /// 不符合就返回None
    fn next_judge<F>(&mut self, predicate: F) -> Option<char>
    where
        F: Fn(&&char) -> bool,
    {
        self.iter.peek().filter(predicate)?;
        self.iter.next()
    }

    /// fn是判断,如果符合就true
    fn peek_judge<F>(&mut self, predicate: F) -> bool
    where
        F: Fn(&&char) -> bool,
    {
        self.iter.peek().filter(predicate).is_some()
    }

    /// 如果下一个字符匹配c 就调用next 并返回这个字符
    /// 不匹配就返回none
    fn next_char_expect(&mut self, c: char) -> Option<char> {
        match self.iter.peek() {
            Some(ch) if *ch == c => self.iter.next(),
            Some(_) => None,
            None => None,
        }
    }

    fn term(&mut self) {
        while self
            .next_judge(|&&t| match t {
                ' ' | '\n' | '\t' => true,
                _ => false,
            })
            .is_some()
        {}
    }
}

impl<'a> Iterator for Laxer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.get_next() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self
                .iter
                .peek()
                .map(|c| Err(Error::Parse(format!("get unexpected char {}", c)))),
            Err(err) => Some(Err(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_iter_test() {
        let laxer = Laxer::new("Select * from nmber != 123.123 and who is null babab thi AS");
        let mut r = String::new();
        for token in laxer {
            match token {
                Ok(token) => r = format!("{} {:?}", r, token),
                Err(e) => eprint!("{}", e),
            }
        }
        println!("r={}", r);
        assert_eq!(r, " Keyword(Select) Asterisk Keyword(From) Ident(\"nmber\") NotEqual Number(\"123.123\") Keyword(And) Ident(\"who\") Keyword(Is) Keyword(Null) Ident(\"babab\") Ident(\"thi\") Keyword(As)".to_string())
    }
}
