// api.js

import axios from "axios";

export const suggestUsers = async (name) => {
    try {
      const response = await axios.post('/api/suggest_users', { username: name });
      return response.data;
    } catch (error) {
      console.error('Error during user suggestion:', error);
      throw error; // Re-throw the error so that the component can handle it
    }
  };

export const popularMovies = async (name) => {
  try {
    const response = await axios.post('/api/popular_movies', { username: name });
    return response.data;
  } catch (error) {
    console.error('Error during user suggestion:', error);
    throw error; // Re-throw the error so that the component can handle it
  }
};

export const ratedMovies = async (name) => {
  try {
    const response = await axios.post('/api/rated_movies', { username: name });
    return response.data;
  } catch (error) {
    console.error('Error during user suggestion:', error);
    throw error; // Re-throw the error so that the component can handle it
  }
};

export const suggestMoviesBasedOnRandom = async (name) => {
  try {
    const response = await axios.post('/api/suggest_movies_based_on_random', { username: name });
    return response.data;
  } catch (error) {
    console.error('Error during user suggestion:', error);
    throw error; // Re-throw the error so that the component can handle it
  }
};

export const suggestMoviesBasedOnActorInMovie = async (name) => {
  try {
    const response = await axios.post('/api/suggest_movies_based_on_actor_in_movie', { username: name });
    return response.data;
  } catch (error) {
    console.error('Error during user suggestion:', error);
    throw error; // Re-throw the error so that the component can handle it
  }
};

export const getGenresBasedOnPopularity = async (name) => {
  try {
    const response = await axios.post('/api/genres_based_on_popularity', { username: name });
    return response.data;
  } catch (error) {
    console.error('Error during user suggestion:', error);
    throw error; // Re-throw the error so that the component can handle it
  }
};

export const suggestLatestMoviesBasedOnActor = async (name) => {
  try {
    const response = await axios.post('/api/suggest_latest_movies_based_on_actor', { username: name });
    return response.data;
  } catch (error) {
    console.error('Error during user suggestion:', error);
    throw error; // Re-throw the error so that the component can handle it
  }
};

export const suggestLatestMoviesBasedOnDirector = async (name) => {
  try {
    const response = await axios.post('/api/suggest_latest_movies_based_on_director', { username: name });
    return response.data;
  } catch (error) {
    console.error('Error during user suggestion:', error);
    throw error; // Re-throw the error so that the component can handle it
  }
};

export const suggestLatestMoviesBasedOnGenre = async (name) => {
  try {
    const response = await axios.post('/api/suggest_latest_movies_based_on_genre', { username: name });
    return response.data;
  } catch (error) {
    console.error('Error during user suggestion:', error);
    throw error; // Re-throw the error so that the component can handle it
  }
};

export const RecommendMovieGenre = async (name) => {
  try {
    const response = await axios.post('/api/recommend_movie_genre', { username: name });
    return response.data;
  } catch (error) {
    console.error('Error during user suggestion:', error);
    throw error; // Re-throw the error so that the component can handle it
  }
};