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
            Keyword::Get => process_get_keyword(tokens),
            Keyword::Put => process_put_keyword(tokens),
            Keyword::Delete => process_delete_keyword(tokens),
            Keyword::Exit => Ok(Command::Exit),
        },
        _ => Err("Expected keyword GET, PUT or DELETE".into()),
    }
}

fn process_put_keyword_with_key(
    ident: String,
    mut tokens: impl Iterator<Item = Token>,
) -> Result<Command, String> {
    let next = tokens.next();
    if next.is_none() {
        return Err("Expected literal after identifier".into());
    }
    let literal = next.unwrap();
    match literal {
        Token::Literal(literal) => {
            let next = tokens.next();
            if next.is_some() {
                return Err("Unexpected token after literal".into());
            }
            Ok(Command::Put(PutCommand(ident, literal)))
        }
        _ => Err("Expected literal after identifier".into()),
    }
}

fn process_put_keyword(mut tokens: impl Iterator<Item = Token>) -> Result<Command, String> {
    let ident = tokens.next();
    if ident.is_none() {
        return Err("Expected identifier after PUT".into());
    }
    let ident = ident.unwrap();
    match ident {
        Token::Ident(ident) => process_put_keyword_with_key(ident, tokens),
        _ => Err("Expected identifier after PUT".into()),
    }
}

fn process_get_keyword(mut tokens: impl Iterator<Item = Token>) -> Result<Command, String> {
    let ident = tokens.next();
    if ident.is_none() {
        return Err("Expected identifier after GET".into());
    }
    let ident = ident.unwrap();
    match ident {
        Token::Ident(ident) => {
            let next = tokens.next();
            if next.is_some() {
                return Err("Unexpected token after identifier".into());
            }
            Ok(Command::Get(GetCommand(ident)))
        }
        _ => Err("Expected identifier after GET".into()),
    }
}

fn process_delete_keyword(mut tokens: impl Iterator<Item = Token>) -> Result<Command, String> {
    let ident = tokens.next();
    if ident.is_none() {
        return Err("Expected identifier after DELETE".into());
    }
    let ident = ident.unwrap();
    match ident {
        Token::Ident(ident) => {
            let next = tokens.next();
            if next.is_some() {
                return Err("Unexpected token after identifier".into());
            }
            Ok(Command::Delete(DeleteCommand(ident)))
        }
        _ => Err("Expected identifier after DELETE".into()),
    }
}
