use crate::command::{Command, DeleteCommand, GetCommand, PutCommand};

pub fn parse_command(input: String) -> Result<Command, String> {
    let tokens = Lexer::new(input).lex()?;
    parse_tokens(tokens.into_iter())
}

#[derive(Debug)]
enum Keyword {
    Get,
    Put,
    Delete,
    Exit,
}

#[derive(Debug)]
enum Token {
    Keyword(Keyword),
    Ident(String),
    Literal(String),
}

#[derive(Debug)]
struct Lexer {
    input: Vec<char>,
    pos: usize,
    buffer: String,
    tokens: Vec<Token>,
}

impl Lexer {
    fn new(input: String) -> Lexer {
        Lexer {
            input: input.chars().collect(),
            pos: 0,
            buffer: String::new(),
            tokens: Vec::new(),
        }
    }

    // TODO: should have used a more iterative approach as opposed to recursive
    fn lex(mut self) -> Result<Vec<Token>, String> {
        let c = self.input.get(self.pos);
        if c.is_none() {
            return Ok(self.tokens);
        }
        let c = c.unwrap();

        if c.is_whitespace() {
            self.pos += 1;
            self.lex()
        } else if c.is_alphanumeric() {
            self.key_or_ident()
        } else if c == &'"' {
            self.literal()
        } else {
            Err(format!("Unexpected character: {}", c))
        }
    }

    fn key_or_ident(mut self) -> Result<Vec<Token>, String> {
        let c = self.input.get(self.pos);
        if c.is_none() {
            return self.key_or_indent();
        }
        let c = c.unwrap();

        if c.is_alphanumeric() {
            self.buffer.push(*c);
            self.pos += 1;
            self.key_or_ident()
        } else if c.is_whitespace() {
            match self.buffer.as_str() {
                "get" | "GET" => {
                    self.tokens.push(Token::Keyword(Keyword::Get));
                }
                "put" | "PUT" => {
                    self.tokens.push(Token::Keyword(Keyword::Put));
                }
                "delete" | "DELETE" => {
                    self.tokens.push(Token::Keyword(Keyword::Delete));
                }
                "exit" | "EXIT" => {
                    self.tokens.push(Token::Keyword(Keyword::Exit));
                }
                _ => {
                    self.tokens.push(Token::Ident(self.buffer.clone()));
                }
            }
            self.buffer.clear();
            self.lex()
        } else {
            Err(format!("Invalid token {}", self.buffer))
        }
    }

    fn key_or_indent(mut self) -> Result<Vec<Token>, String> {
        if self.buffer.is_empty() {
            return Ok(self.tokens);
        }
        match self.buffer.as_str() {
            "get" | "GET" => {
                self.tokens.push(Token::Keyword(Keyword::Get));
            }
            "put" | "PUT" => {
                self.tokens.push(Token::Keyword(Keyword::Put));
            }
            "delete" | "DELETE" => {
                self.tokens.push(Token::Keyword(Keyword::Delete));
            }
            "exit" | "EXIT" => {
                self.tokens.push(Token::Keyword(Keyword::Exit));
            }
            _ => {
                self.tokens.push(Token::Ident(self.buffer.clone()));
            }
        }
        Ok(self.tokens)
    }

    fn literal(mut self) -> Result<Vec<Token>, String> {
        let c = self.input.get(self.pos);
        if c.is_none() {
            return Err("Unexpected end of input".into());
        }
        let c = c.unwrap();

        match c {
            '"' => {
                self.pos += 1;
                self.tokens.push(Token::Literal(self.buffer.clone()));
                self.buffer.clear();
                self.lex()
            }
            '\\' => {
                self.pos += 1;
                self.literal_in_escape()
            }
            _ => {
                self.buffer.push(*c);
                self.pos += 1;
                self.literal()
            }
        }
    }

    fn literal_in_escape(mut self) -> Result<Vec<Token>, String> {
        let c = self.input.get(self.pos);
        if c.is_none() {
            return Err("Unexpected end of input".into());
        }
        let c = c.unwrap();
        match c {
            '"' | 'n' | 't' | '\\' => {
                self.buffer.push(*c);
                self.pos += 1;
                self.literal()
            }
            _ => Err(format!("Invalid escape character: {}", c)),
        }
    }
}

fn parse_tokens(mut tokens: impl Iterator<Item = Token>) -> Result<Command, String> {
    let next = tokens.next();
    if next.is_none() {
        return Err("Unexpected end of input".into());
    }
    let next = next.unwrap();
    match next {
        Token::Keyword(keyword) => match keyword {
            Keyword::Get => process_get_keyword(&mut tokens),
            Keyword::Put => process_put_keyword(&mut tokens),
            Keyword::Delete => process_delete_keyword(&mut tokens),
            Keyword::Exit => Ok(Command::Exit),
        },
        _ => Err("Expected keyword GET, PUT or DELETE".into()),
    }
}

fn parse_identifier(tokens: &mut impl Iterator<Item = Token>, keyword: &str) -> Result<String, String> {
    match tokens.next() {
        Some(Token::Ident(ident)) => Ok(ident),
        _ => Err(format!("Expected identifier after {}", keyword)),
    }
}

fn process_put_key_word_with_key(ident: String, tokens: &mut impl Iterator<Item = Token>) -> Result<Command, String> {
    match tokens.next() {
        Some(Token::Literal(literal)) => {
            if tokens.next().is_some() {
                return Err("Unexpected token after literal".to_string());
            }
            Ok(Command::Put(PutCommand(ident, literal)))
        }
        _ => Err("Expected literal after identifier".to_string()),
    }
}

fn process_put_keyword(tokens: &mut impl Iterator<Item = Token>) -> Result<Command, String> {
    let ident = parse_identifier(tokens, "PUT")?;
    process_put_key_word_with_key(ident, tokens)
}

fn process_get_keyword(tokens: &mut impl Iterator<Item = Token>) -> Result<Command, String> {
    let ident = parse_identifier(tokens, "GET")?;
    if tokens.next().is_some() {
        return Err("Unexpected token after identifier".to_string());
    }
    Ok(Command::Get(GetCommand(ident)))
}

fn process_delete_keyword(tokens: &mut impl Iterator<Item = Token>) -> Result<Command, String> {
    let ident = parse_identifier(tokens, "DELETE")?;
    if tokens.next().is_some() {
        return Err("Unexpected token after identifier".to_string());
    }
    Ok(Command::Delete(DeleteCommand(ident)))
}
