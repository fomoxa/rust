use cyclone_attributes::*;

#[network]
#[codec(edge)]
pub struct Player {
    #[network(u32)]
    #[codec(edge)]
    pub hp: u32,

    #[network(string)]
    #[codec(edge)]
    pub name: String,
}

#[network]
#[codec(client)]
pub struct PlayerInput {
    #[network(f32)]
    #[codec(client)]
    pub x: f32,

    #[network(f32)]
    #[codec(client)]
    pub z: f32,
}

